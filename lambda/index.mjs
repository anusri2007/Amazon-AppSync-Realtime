import {
  DynamoDBClient,
  PutItemCommand,
  DeleteItemCommand,
  UpdateItemCommand,
  ScanCommand
} from "@aws-sdk/client-dynamodb";

import {
  ApiGatewayManagementApiClient,
  PostToConnectionCommand
} from "@aws-sdk/client-apigatewaymanagementapi";

const dynamo = new DynamoDBClient({});

const CONNECTIONS_TABLE = "WebSocketConnections";
const ORDERS_TABLE = "Orders";
const RESTAURANTS_TABLE = "Restaurants";

export const handler = async (event) => {

  console.log("EVENT:", JSON.stringify(event));

  const routeKey = event.requestContext?.routeKey;
  const connectionId = event.requestContext?.connectionId;

  // =====================================================
  // API Gateway WebSocket Management API
  // =====================================================

  const domainName = event.requestContext?.domainName;
  const stage = event.requestContext?.stage;

  const apiGateway = new ApiGatewayManagementApiClient({
    endpoint: `https://${domainName}/${stage}`
  });

  // =====================================================
  // CONNECT
  // =====================================================

  if (routeKey === "$connect") {

    console.log("Client connected:", connectionId);

    await dynamo.send(
      new PutItemCommand({
        TableName: CONNECTIONS_TABLE,
        Item: {
          connectionId: {
            S: connectionId
          }
        }
      })
    );

    return {
      statusCode: 200,
      body: "Connected"
    };
  }

  // =====================================================
  // DISCONNECT
  // =====================================================

  if (routeKey === "$disconnect") {

    console.log("Client disconnected:", connectionId);

    await dynamo.send(
      new DeleteItemCommand({
        TableName: CONNECTIONS_TABLE,
        Key: {
          connectionId: {
            S: connectionId
          }
        }
      })
    );

    return {
      statusCode: 200,
      body: "Disconnected"
    };
  }

  // =====================================================
  // UPDATE RESTAURANT
  // =====================================================

  if (routeKey === "updateRestaurant") {

    try {

      const body = JSON.parse(event.body || "{}");

      console.log("Restaurant update:", body);

      const restaurantId = body.restaurantId;
      const status = body.status;

      if (!restaurantId || !status) {

        return {
          statusCode: 400,
          body: JSON.stringify({
            message: "restaurantId and status are required"
          })
        };
      }

      // ---------------------------------------------
      // Update Restaurants table
      // ---------------------------------------------

      const result = await dynamo.send(
        new UpdateItemCommand({

          TableName: RESTAURANTS_TABLE,

          Key: {
            restaurantId: {
              S: restaurantId
            }
          },

          UpdateExpression: "SET #status = :status",

          ExpressionAttributeNames: {
            "#status": "status"
          },

          ExpressionAttributeValues: {
            ":status": {
              S: status
            }
          },

          ReturnValues: "ALL_NEW"
        })
      );

      console.log(
        "Restaurant updated:",
        result.Attributes
      );

      // ---------------------------------------------
      // Broadcast to ALL connected websites
      // ---------------------------------------------

      const connections = await dynamo.send(
        new ScanCommand({
          TableName: CONNECTIONS_TABLE
        })
      );

      const message = {
        action: "restaurantUpdated",
        restaurantId: restaurantId,
        status: status
      };

      for (const item of connections.Items || []) {

        const id = item.connectionId.S;

        try {

          await apiGateway.send(
            new PostToConnectionCommand({

              ConnectionId: id,

              Data: Buffer.from(
                JSON.stringify(message)
              )

            })
          );

          console.log(
            "Restaurant update sent to:",
            id
          );

        } catch (error) {

          console.log(
            "Could not send to:",
            id
          );

          // Remove stale connection
          if (
            error.name === "GoneException" ||
            error.$metadata?.httpStatusCode === 410
          ) {

            await dynamo.send(
              new DeleteItemCommand({
                TableName: CONNECTIONS_TABLE,
                Key: {
                  connectionId: {
                    S: id
                  }
                }
              })
            );
          }
        }
      }

      return {

        statusCode: 200,

        body: JSON.stringify({
          message: "Restaurant status updated",
          restaurantId: restaurantId,
          status: status
        })

      };

    } catch (error) {

      console.error(
        "Restaurant update error:",
        error
      );

      return {

        statusCode: 500,

        body: JSON.stringify({
          message: "Failed to update restaurant",
          error: error.message
        })

      };
    }
  }

  // =====================================================
  // UPDATE ORDER
  // =====================================================

  if (routeKey === "updateOrder") {

    try {

      const body = JSON.parse(event.body || "{}");

      const orderId = body.orderId;
      const status = body.status;

      if (!orderId || !status) {

        return {
          statusCode: 400,
          body: JSON.stringify({
            message: "orderId and status are required"
          })
        };
      }

      // ---------------------------------------------
      // Update Orders table
      // ---------------------------------------------

      await dynamo.send(
        new UpdateItemCommand({

          TableName: ORDERS_TABLE,

          Key: {
            id: {
              S: orderId
            }
          },

          UpdateExpression: "SET #status = :status",

          ExpressionAttributeNames: {
            "#status": "status"
          },

          ExpressionAttributeValues: {
            ":status": {
              S: status
            }
          },

          ReturnValues: "ALL_NEW"
        })
      );

      // ---------------------------------------------
      // Broadcast order update
      // ---------------------------------------------

      const connections = await dynamo.send(
        new ScanCommand({
          TableName: CONNECTIONS_TABLE
        })
      );

      const message = {

        action: "orderUpdated",

        orderId: orderId,

        status: status

      };

      for (const item of connections.Items || []) {

        const id = item.connectionId.S;

        try {

          await apiGateway.send(
            new PostToConnectionCommand({

              ConnectionId: id,

              Data: Buffer.from(
                JSON.stringify(message)
              )

            })
          );

        } catch (error) {

          if (
            error.name === "GoneException" ||
            error.$metadata?.httpStatusCode === 410
          ) {

            await dynamo.send(
              new DeleteItemCommand({
                TableName: CONNECTIONS_TABLE,
                Key: {
                  connectionId: {
                    S: id
                  }
                }
              })
            );
          }
        }
      }

      return {

        statusCode: 200,

        body: JSON.stringify({

          message: "Order updated",

          orderId: orderId,

          status: status

        })

      };

    } catch (error) {

      console.error(error);

      return {

        statusCode: 500,

        body: JSON.stringify({

          message: "Failed to update order",

          error: error.message

        })

      };
    }
  }

  // =====================================================
  // UNKNOWN ROUTE
  // =====================================================

  return {

    statusCode: 400,

    body: JSON.stringify({

      message: "Unknown route",

      route: routeKey

    })

  };
};