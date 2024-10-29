import unittest
from unittest.mock import MagicMock, patch
import consumer  
import json

class TestConsumer(unittest.TestCase):

    def test_retrieve_and_sort(self):
        # Mock response from S3
        response = {'Contents': [{'Key': 'b'}, {'Key': 'a'}, {'Key': 'c'}]}
        sorted_keys = consumer.retrieve_and_sort(response)
        self.assertEqual(sorted_keys, ['a', 'b', 'c'])

    def test_write_to_database(self):
        # Mock session and dependencies
        session_mock = MagicMock()
        parsed_args_mock = MagicMock(write_database="test_table")
        s3_client_mock = MagicMock()
        sorted_keys = ["test_key"]

        json_data = {
            "type": "create",
            "owner": "John Doe",
            "widgetId": "123",
            "otherAttributes": [{"name": "size", "value": "5"}]
        }

        with patch("consumer.logger") as logger_mock:
            consumer.write_to_database(json_data, session_mock, parsed_args_mock, sorted_keys, "test_key", s3_client_mock)
            logger_mock.info.assert_called_with("Item written to DynamoDB table test_table")

            # Confirm item structure after processing
            self.assertEqual(json_data["id"], "John Doe")
            self.assertEqual(json_data["size"], "5")
            self.assertNotIn("otherAttributes", json_data)

    def test_write_to_s3(self):
        # Mock S3 client and dependencies
        s3_client_mock = MagicMock()
        parsed_args_mock = MagicMock(write_bucket="test_write_bucket")
        sorted_keys = ["test_key"]
        widget_key = "widgets/john-doe/123"
        widget_json = json.dumps({"type": "create", "id": "john-doe", "widgetId": "123"})

        with patch("consumer.logger") as logger_mock:
            consumer.write_to_s3(parsed_args_mock, widget_key, widget_json, s3_client_mock, "test_key", sorted_keys)
            s3_client_mock.put_object.assert_called_with(Bucket="test_write_bucket", Key=widget_key, Body=widget_json)
            logger_mock.info.assert_called_with(f"Stored Widget in test_write_bucket with key {widget_key}.")

if __name__ == '__main__':
    unittest.main()
