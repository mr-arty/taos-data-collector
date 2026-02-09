"""
server_generate_all.py

OPC UA server that generates test data for all 16 sensors
defined in server_connect.py
"""

import asyncio
import random
from asyncua import Server, ua


# Sensor definitions with realistic value ranges
# Format: (node_name, display_name, min_val, max_val)
SENSOR_DEFINITIONS = [
    ("Temperature", "Temperature", 20.0, 30.0),
    ("Humidity", "Humidity", 40.0, 60.0),
    ("Pressure", "Pressure", 1000.0, 1020.0),
    ("Viscocity", "Viscocity", 0.5, 5.0),
    ("OutsideTemperature", "OutsideTemperature", -10.0, 40.0),
    ("Station1Pressure", "Station1Pressure", 1000.0, 1020.0),
    ("Station2Pressure", "Station2Pressure", 1000.0, 1020.0),
    ("Station3Pressure", "Station3Pressure", 1000.0, 1020.0),
    ("Station4Pressure", "Station4Pressure", 1000.0, 1020.0),
    ("Station5Pressure", "Station5Pressure", 1000.0, 1020.0),
    ("Station6Pressure", "Station6Pressure", 1000.0, 1020.0),
    ("Station7Pressure", "Station7Pressure", 1000.0, 1020.0),
    ("Station8Pressure", "Station8Pressure", 1000.0, 1020.0),
    ("Station9Pressure", "Station9Pressure", 1000.0, 1020.0),
    ("Station10Pressure", "Station10Pressure", 1000.0, 1020.0),
    ("Voltage", "Voltage", 110.0, 240.0),
]


async def main():
    # Initialize the OPC UA server
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/sensor_server/")
    server.set_server_name("Dummy Sensor OPC UA Server")

    # Register a namespace
    uri = "http://examples.sensor.opcua"
    idx = await server.register_namespace(uri)

    # Get Objects node and add a folder for sensors
    objects = server.nodes.objects
    sensor_folder = await objects.add_folder(idx, "Sensors")

    # Create all sensor variables
    sensor_vars = []
    for node_name, display_name, min_val, max_val in SENSOR_DEFINITIONS:
        initial_value = (min_val + max_val) / 2
        var = await sensor_folder.add_variable(
            ua.NodeId(node_name, idx, ua.NodeIdType.String),
            display_name,
            initial_value,
            varianttype=ua.VariantType.Double
        )
        await var.set_writable()
        sensor_vars.append((var, node_name, min_val, max_val))

    print(f"Starting OPC UA server at opc.tcp://0.0.0.0:4840/sensor_server/")
    print(f"Registered {len(sensor_vars)} sensors")
    await server.start()

    try:
        while True:
            # Generate and update all sensor values
            updates = []
            for var, name, min_val, max_val in sensor_vars:
                new_value = random.uniform(min_val, max_val)
                await var.write_value(new_value)
                updates.append(f"{name}={new_value:.2f}")

            print(f"Updated {len(updates)} sensors: {', '.join(updates[:3])}... (and {len(updates)-3} more)")

            # Wait before next update
            await asyncio.sleep(10)
    finally:
        await server.stop()
        print("OPC UA server stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt received, stopping server...")
