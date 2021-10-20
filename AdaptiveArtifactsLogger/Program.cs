using MQTTnet.Client;
using MQTTnet;
using System;
using System.Threading;
using MQTTnet.Client.Options;
using System.Threading.Tasks;
using System.Collections;
using System.Collections.Generic;
using MySql.Data.MySqlClient;
using System.Text;
using Newtonsoft.Json.Linq;
using MQTTnet.Client.Subscribing;

namespace MQTTListener
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "MQTT Logger V1.4";
            //string variable to hold 
            string JsonMessage = "";
            //Establishing connection to MQTT Server
            Console.Write("Connecting to the MQTT server...");
            var client = new MqttFactory().CreateMqttClient();
            var clientOptions = new MqttClientOptionsBuilder()
                .WithClientId("MQTT Logger Client")
                .WithTcpServer("66.94.100.229", 1883)
                .Build();
            client.ConnectAsync(clientOptions);

            // TO TEST:
            //*******************************************************************************************
            //var date = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            //var sensorName = "false_sensor";
            //var temp = 30.0;
            //var tempValType = "C";
            //List<string> SqlCommands = new List<string>();
            //SqlCommands.Add($"INSERT INTO iot_events" +
            //                $"(event_date, sensor_id, sensor_value, event_type)" +
            //                $"VALUES " +
            //                $"('{date}', '{sensorName}', '{temp}', '{tempValType}')");
            //SqlExecuteNonQuery(SqlCommands);
            //********************************************************************************************


            client.UseDisconnectedHandler(async e =>
            {
                Console.WriteLine("### DISCONNECTED FROM SERVER ###");
                await Task.Delay(TimeSpan.FromSeconds(5));
                try
                {
                    await client.ConnectAsync(clientOptions, CancellationToken.None); // Since 3.0.5 with CancellationToken
                }
                catch
                {
                    Console.WriteLine("### RECONNECTING FAILED ###");
                }
            });
            //Subscribing to the event when a message is published 
            client.UseApplicationMessageReceivedHandler(e =>
            {
                try
                {
                    JsonMessage = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                    Console.WriteLine("Message received, Message contents: " + JsonMessage);
                    //Deserializing the JSON string
                    JObject jsonObj = JObject.Parse(JsonMessage);

                    //Getting the temperature value
                    string sensorName = jsonObj.Property("SensorName").Value.ToString();
                    double temperature = Convert.ToDouble(jsonObj.Property("Temperature").Value.ToString());
                    string tempValType = "TMP";
                    string date = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

                    double humidity;
                    string hmdValType = "HMD";
                    try
                    {
                        //Getting the humidity value
                        humidity = Convert.ToDouble(jsonObj.Property("Humidity").Value.ToString());
                    }
                    catch
                    {
                        humidity = 0.00;
                    }
                    double pressure;
                    string prsValType = "PRS";
                    try
                    {
                        //Getting the pressure value
                        pressure = Convert.ToDouble(jsonObj.Property("Pressure").Value.ToString());
                    }
                    catch
                    {
                        pressure = 0.00;
                    }
                    Console.WriteLine(sensorName + " sensor says that the temperature value is " + temperature
                                      + ", the humidity is " + humidity + ", and the pressure is " + pressure);

                    List<string> SqlCommands = new List<string>();
                    SqlCommands.Add($"INSERT INTO IoTValues" +
                                    $"(Date, SensorName, SensorValue, ValueType)" +
                                    $"VALUES " +
                                    $"('{date}', '{sensorName}', '{temperature}', '{tempValType}')");
                    if (humidity != 0)
                    {
                        SqlCommands.Add($"INSERT INTO IoTValues" +
                                        $"(Date, SensorName, SensorValue, ValueType)" +
                                        $"VALUES " +
                                        $"('{date}', '{sensorName}', '{humidity}', '{hmdValType}')");
                    }
                    if (pressure != 0)
                    {
                        SqlCommands.Add($"INSERT INTO IoTValues" +
                                        $"(Date, SensorName, SensorValue, ValueType)" +
                                        $"VALUES " +
                                        $"('{date}', '{sensorName}', '{pressure}', '{prsValType}')");
                    }

                    SqlExecuteNonQuery(SqlCommands);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error occured: " + ex.Message);
                }
                Console.WriteLine("******************************************************************************\n");
            });

            while (client.IsConnected == false)
            {
                Thread.Sleep(200);
            }
            Console.Write("Connected!\nClient Name: " + clientOptions.ClientId + "\n");

            //Adding a subscription to /sensors/general topic
            Console.Write("Subscribing to /sensors/values topic on server...");
            client.UseConnectedHandler(async e =>
            {
                Console.WriteLine("### CONNECTED WITH SERVER ###");

                // Subscribe to a topic
                await client.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic("/sensors/values").Build());

                Console.WriteLine("### SUBSCRIBED ###");
            });
            Console.Write("Successfully Subscribed!\n\n");
            while (client.IsConnected == true)
            {
                Thread.Sleep(10000);
            }
        }
        private static IList SqlExecuteNonQuery(List<string> SqlStatements)
        {
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder();
            builder.Server = "66.94.100.229";
            builder.UserID = "username";
            builder.Password = "root123";
            builder.Database= "adaptive_artifacts";
            MySqlConnection connection = new MySqlConnection(builder.ConnectionString);
            {
                connection.Open();

                List<string> dataOutput = new List<string>();

                foreach (string statement in SqlStatements)
                {
                    var command = connection.CreateCommand();
                    command.CommandText = statement;
                    dataOutput.Add(command.ExecuteNonQuery().ToString());
                }
                connection.Close();
                return dataOutput;
            }
        }
    }
}
