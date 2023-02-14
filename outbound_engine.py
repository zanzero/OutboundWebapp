import boto3
import sqlite3
import datetime
import time
from aws_cred import aws_access_key_id, aws_secret_access_key


class OutboundEngine:
    db_name = "aws_outbound_db.db"
    table_name = "outbound_called"

    client = boto3.client("connect", region_name="ap-southeast-1", aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    option_number_of_reuse = 3
    option_repeat_x_minutes = 60
    option_repeat_yes_or_no = "Run"

    def call_db(self, op, **kwargs):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()

        if op == "normal":
            query = kwargs['query']
            print(query)

        elif op == "insert":
            query = f"INSERT INTO {self.table_name} (apiexec, customernumber, datetime, list_name) \
            VALUES ('{kwargs['api_exec']}', '{kwargs['customer_number']}', '{kwargs['date_time_now']}', '{kwargs['list_name']}')"
            print(query)

        elif op == "update":
            query = f"UPDATE {self.table_name} SET 'update' = '{kwargs['update']}', number_of_calls = {kwargs['calls']} \
            WHERE id = {kwargs['phone_id']}"
            print(query)

        else:
            print("Error, What did you say?")

        cursor.execute(query)
        connection.commit()
        connection.close()
        return cursor

    def check_agent(self):
        try:
            response = OutboundEngine.client.get_current_metric_data(
                InstanceId='91aeda1c-da5c-4dcd-bff1-9c6f9ce5d87f',
                Filters={
                    'Queues': [
                        '9c2adc06-6c47-4bbd-9446-c5cd0e35ee9b',
                    ],
                },
                CurrentMetrics=[
                    {
                        'Name': 'AGENTS_AVAILABLE',
                        'Unit': 'COUNT'
                    },
                ],
            )
            return response["MetricResults"][0].get("Collections")[0].get("Value")
        except Exception:
            return 0.0

    def outbound_call(self, destination_number):
        destination_phone_number = destination_number
        contact_flow_id = 'c2015656-22dd-4a6d-8a87-28ad7ea80373'
        instance_id = '91aeda1c-da5c-4dcd-bff1-9c6f9ce5d87f'
        source_phone_number = '+6620806061'

        response = self.client.start_outbound_voice_contact(
            DestinationPhoneNumber=destination_phone_number,
            ContactFlowId=contact_flow_id,
            InstanceId=instance_id,
            SourcePhoneNumber=source_phone_number
        )
        print(destination_number + " Called")
        print(str(response))

    def run_list(self, df, list_name):
        for customer_number in df['Address']:
            switch = True
            customer_number_str = str(customer_number)
            customer_number = "+66" + customer_number_str
            waiting_time = int()

            while switch:
                if self.check_agent() >= 1:
                    self.outbound_call(customer_number)
                    self.call_db(op="insert", api_exec="Y", customer_number="0" + customer_number_str,
                                 date_time_now=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                 list_name=list_name)
                    time.sleep(30)
                    switch = False
                else:
                    # time.sleep(10)
                    waiting_time += 1
                    if waiting_time == 10:
                        self.call_db(op="insert", api_exec="N", customer_number="0" + customer_number_str,
                                     date_time_now=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                     list_name=list_name)
                        waiting_time = 0
                        switch = False

    def re_use(self, reuse_list):
        for i in reuse_list:
            switch = True
            customer_number = i[4]
            phone_id = i[0]

            customer_number_str = str(customer_number)
            customer_number = "+66" + customer_number_str
            waiting_time = int()

            while switch:
                if self.check_agent() >= 1:
                    self.outbound_call(customer_number)
                    self.call_db(op="update", phone_id=phone_id,
                                 update=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                 calls='number_of_calls+1')
                    time.sleep(30)
                    switch = False

                else:
                    # time.sleep(10)
                    waiting_time += 1
                    if waiting_time == 10:
                        self.call_db(op="update", phone_id=phone_id,
                                     update=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                     calls="number_of_calls")
                        waiting_time = 0
                        switch = False


OutboundNG = OutboundEngine()
