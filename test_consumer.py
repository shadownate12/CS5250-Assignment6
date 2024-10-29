import unittest
from unittest.mock import MagicMock
import consumer  # Assuming your script is saved as consumer.py

class TestConsumer(unittest.TestCase):

    def test_retrieve_and_sort(self):
        # Mock response from S3
        response = {'Contents': [{'Key': 'b'}, {'Key': 'a'}, {'Key': 'c'}]}
        sorted_keys = consumer.retrieve_and_sort(response)
        self.assertEqual(sorted_keys, ['a', 'b', 'c'])

    def test_process_json_data(self):
        json_data = {
            "type": "create",
            "owner": "John Doe",
            "widgetId": "123",
            "otherAttributes": [{"name": "size", "value": "5"}]
        }
        processed_data = consumer.process_json_data(json_data)
        self.assertEqual(processed_data["id"], "john-doe")
        self.assertEqual(processed_data["size"], "5")
        self.assertNotIn("otherAttributes", processed_data)

    def test_write_to_s3(self):
        s3_client_mock = MagicMock()
        response = consumer.write_to_s3(s3_client_mock, "bucket", "key", "body")
        s3_client_mock.put_object.assert_called_with(Bucket="bucket", Key="key", Body="body")

    def test_write_to_dynamodb(self):
        dynamo_client_mock = MagicMock()
        response = consumer.write_to_dynamodb(dynamo_client_mock, "table", {"id": "123", "type": "create"})
        dynamo_client_mock.put_item.assert_called_with(TableName="table", Item={"id": "123", "type": "create"})

if __name__ == '__main__':
    unittest.main()
