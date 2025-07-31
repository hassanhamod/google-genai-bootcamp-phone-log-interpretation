from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from google.cloud import bigtable
from google.cloud.bigtable.row_set import RowSet

import google.auth


APP_NAME = "phone_activity_app"
USER_ID = "1234"
SESSION_ID = "session1234"

def get_phone_logs(patient_id: str, start_time: int, end_time: int, logType: str):
    """
    Retrieves phone user activity data for a given patient id within a time range. In BT, UserActivityRecord is also known as phone user activity.
    Contents of a user activity record are json-formatted, and contain the following keys:
    
      "Stream": Enum identifier for the device and operating system,
      "RecordedSystemTime": UTC representation of the recorded display time,
      "RecordedDisplayTime": datetimeoffset expressed as string of the even that occurred on the device,
      "UseractivityType": category of log types,
      "UseractivitySubType": subcategory of UseractivityType, there is a one-to-many mapping of UseractivityType to UseractivitySubType,
      "Data": raw logs of the event expressed as a key-value pair in JSON format,
      "TransmitterNumber": unique identifier for transmitter device,
      "RecordType": UserActivityRecord is the only supported RecordType of the available data

    Args:
        patient_id (str): a UUID for a given patient.
        start_time (int): a timestamp in unix seconds
        end_time (int): a timestamp in unix seconds
        logType (str): string enum that defines which records to return
        
    Returns:
        string: JSON-formatted data of phone user activity records, or None if an error occurs.
    """
    
    project_id="qwiklabs-asl-01-e660751acd56"
    instance_id="phonelogs"
    table_id="phone_user_activity"
    
     # Create a Cloud Bigtable client.
    client = bigtable.Client(project=project_id)

    # Connect to an existing Cloud Bigtable instance.
    instance = client.instance(instance_id)

    # Open an existing table.
    table = instance.table(table_id)

    start_key = f"{patient_id}#UserActivityRecord#{start_time}"
    end_key = f"{patient_id}#UserActivityRecord#{end_time}"
    
    column_family_id = "raw"
    column_id = "Raw".encode("utf-8")
    
    row_set = RowSet()
    row_set.add_row_range_from_keys(start_key, end_key)
    
    rows = table.read_rows(row_set=row_set)
    readableRows = []
    for row in rows:
        readableRow = row.cells[column_family_id][column_id][0].value.decode("utf-8")
        #print(readableRow)
        readableRows.append(readableRow)
    
    print(f"row count: {len(readableRows)}")    
    return readableRows
    
root_agent = Agent(
   model="gemini-2.0-flash",
   name="bigtable_agent",
   description=(
       "Agent that answers questions about BigTable data by executing row reads."
   ),
   instruction=""" You are a data analysis agent with access to Bigtable. Summarize data for a given patient and answer user's questions about the logs. Additionally, you will be asked to identify spikes in logs related to errors, alerts, or crashes. Spikes can be identified as an abnormally large record count of the same useractivitysubtype within a short period of time. Provide a record count and a time range for the data when a spike is detected. The user will provide a human-readable date or datetime, you are expected to convert that time into unix seconds. At minimum, the date should contain a day,  month, and year. If no timestamp is provided, assume midnight to 11:59pm. Do not ask about RecordTypes or UserActivityRecords as you can assume that all data is considered UserActivityRecords. If the user asks about transmitter issues, records with a UserActivityType with Transmitter and Displaying Screen will have the information you need.

   """,
   tools=[
       get_phone_logs
   ],
)


# Session and Runner
async def setup_session_and_runner():
    session_service = InMemorySessionService()
    session = await session_service.create_session(app_name=APP_NAME, user_id=USER_ID, session_id=SESSION_ID)
    runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)
    return session, runner

# Agent Interaction
async def call_agent_async():
    content = types.Content(role='user', parts=[types.Part(text=query)])
    session, runner = await setup_session_and_runner()
    events = runner.run_async(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    async for event in events:
        if event.is_final_response():
            final_response = event.content.parts[0].text
            print("Agent Response: ", final_response)


# Note: In Colab, you can directly use 'await' at the top level.
# If running this code as a standalone Python script, you'll need to use asyncio.run() or manage the event loop.
#await call_agent_async("get phone user activity logs")