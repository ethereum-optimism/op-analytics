import unittest
from unittest.mock import Mock, patch
from datetime import date

from op_analytics.datapipeline.etl.blockbatchload.insert import (
    InsertTask,
    BlockBatch,
    ClickHouseBlockBatchETL,
)


class TestInsertTask(unittest.TestCase):
    def setUp(self):
        self.dataset = ClickHouseBlockBatchETL(
            input_root_paths=["blockbatch/native_transfers/native_transfers_v1"],
            output_root_path="blockbatch/native_transfers/native_transfers_v1",
            enforce_non_zero_row_count=False,
        )
        self.blockbatch = BlockBatch(
            chain="worldchain",
            dt=date(2025, 1, 15),
            min_block=16020800,
            partitioned_path="chain=worldchain/dt=2025-01-15/000016020800.parquet",
        )
        self.insert_task = InsertTask(dataset=self.dataset, blockbatch=self.blockbatch)

    @patch("op_analytics.datapipeline.etl.blockbatchload.insert.init_gcsfs_client")
    def test_validate_gcs_input_files_success(self, mock_init_gcsfs_client):
        """Test successful GCS file validation."""
        mock_gcs_client = Mock()
        mock_gcs_client.exists.return_value = True
        mock_init_gcsfs_client.return_value = mock_gcs_client

        # Should not raise an exception
        self.insert_task.validate_gcs_input_files()

        # Verify the expected GCS path was checked
        expected_path = "oplabs-tools-data-sink/blockbatch/native_transfers/native_transfers_v1/chain=worldchain/dt=2025-01-15/000016020800.parquet"
        mock_gcs_client.exists.assert_called_once_with(expected_path)

    @patch("op_analytics.datapipeline.etl.blockbatchload.insert.init_gcsfs_client")
    def test_validate_gcs_input_files_missing_file(self, mock_init_gcsfs_client):
        """Test GCS file validation when file is missing."""
        mock_gcs_client = Mock()
        mock_gcs_client.exists.return_value = False
        mock_init_gcsfs_client.return_value = mock_gcs_client

        # Should raise FileNotFoundError
        with self.assertRaises(FileNotFoundError) as context:
            self.insert_task.validate_gcs_input_files()

        self.assertIn("Required GCS input file does not exist", str(context.exception))

    @patch("op_analytics.datapipeline.etl.blockbatchload.insert.init_gcsfs_client")
    def test_validate_gcs_input_files_gcs_error(self, mock_init_gcsfs_client):
        """Test GCS file validation when GCS client raises an error."""
        mock_gcs_client = Mock()
        mock_gcs_client.exists.side_effect = Exception("GCS connection error")
        mock_init_gcsfs_client.return_value = mock_gcs_client

        # Should raise the original exception
        with self.assertRaises(Exception) as context:
            self.insert_task.validate_gcs_input_files()

        self.assertIn("GCS connection error", str(context.exception))


if __name__ == "__main__":
    unittest.main()
