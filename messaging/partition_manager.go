package messaging

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/microtest/telemetry"
)

type BlobStorage struct {
	ContainerURL azblob.ContainerURL
}

type LeaseManager struct {
	containerURL    azblob.ContainerURL
	nextPartitionID int
}

// Creates a new BlobStorage instance
func NewBlobStorage(accountName, accountKey, containerName string) (*BlobStorage, error) {
	// Ensure account name, account key, and container name are provided
	if accountName == "" || accountKey == "" || containerName == "" {
		telemetry.TrackTrace("PartitionManager::NewBlobStorage::Missing required parameters", telemetry.Error, nil)
		return nil, errors.New("missing required parameters")
	}

	fmt.Println("Print env variables")
	fmt.Println("STORAGE_ACCOUNT_NAME=", accountName)
	fmt.Println("STORAGE_CONNECTION_STRING=", accountKey)
	fmt.Println("PARTITION_LEASE_CONTAINER=", containerName)

	// Create Blob Storage client
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		telemetry.TrackTrace("PartitionManager::NewBlobStorage::Failed to create shared key credential", telemetry.Error, nil)
		return nil, err
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, _ := url.Parse("https://" + accountName + ".blob.core.windows.net/" + containerName)
	containerURL := azblob.NewContainerURL(*u, pipeline)

	return &BlobStorage{ContainerURL: containerURL}, nil
}

// Stores the checkpoint for a partition in Blob Storage
func (bs *BlobStorage) StoreCheckpoint(partitionID, offset string) error {
	// Create blob URL for storing checkpoint
	blobURL := bs.ContainerURL.NewBlockBlobURL(partitionID + ".checkpoint")

	// Convert offset to byte slice
	data := []byte(offset)

	// Upload checkpoint to Blob Storage
	_, err := azblob.UploadBufferToBlockBlob(context.Background(), data, blobURL, azblob.UploadToBlockBlobOptions{})
	if err != nil {
		handleError("PartitionMgr::StoreCheckpoint::Error uploading checkpoint", err)
		return err
	}

	return nil
}

// Retrieves the checkpoint for a partition from Blob Storage
func (bs *BlobStorage) GetCheckpoint(partitionID string) (string, error) {
	// Get blob URL for checkpoint
	blobURL := bs.ContainerURL.NewBlockBlobURL(partitionID + ".checkpoint")

	// Download checkpoint from Blob Storage
	resp, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		handleError("PartitionMgr::GetCheckpoint::Error downloading checkpoint", err)
		return "", err
	}

	bodyStream := resp.Body(azblob.RetryReaderOptions{})
	defer bodyStream.Close()

	// Read checkpoint from response body
	data := make([]byte, resp.ContentLength())
	_, err = bodyStream.Read(data)
	if err != nil {
		handleError("PartitionMgr::GetCheckpoint::Error reading body stream", err)
		return "", err
	}

	return string(data), nil
}

// Creates a new LeaseManager instance
func NewLeaseManager(accountName, accountKey, containerName string) (*LeaseManager, error) {
	// Create Blob Storage client
	blobStorage, err := NewBlobStorage(accountName, accountKey, containerName)

	if err != nil {
		telemetry.TrackTrace("PartitionMgr::NewLeaseManager::Failed to create Blob Storage client", telemetry.Error, map[string]string{"Error": err.Error()})
		return nil, err
	}

	return &LeaseManager{
		containerURL:    blobStorage.ContainerURL,
		nextPartitionID: 0,
	}, nil
}

// Acquires a lease for a partition
func AcquireLease(lm *LeaseManager, numPartitions int, leaseDuration int32) (string, error) {
	// Get the next partition ID to attempt to acquire a lease for
	partitionID := strconv.Itoa(lm.nextPartitionID)

	// Increment the next partition ID for the next call
	lm.nextPartitionID = (lm.nextPartitionID + 1) % numPartitions

	fmt.Println("PartitionMgr::AcquireLease::ConsumerID: ", partitionID)
	fmt.Println("PartitionMgr::AcquireLease::NumPartitions: ", numPartitions)
	fmt.Println("PartitionMgr::AcquireLease::LeaseDuration: ", leaseDuration)

	// Try to acquire the lease for the partition
	blobURL := lm.containerURL.NewBlockBlobURL(partitionID)
	leaseID := "lease-" + partitionID
	_, err := blobURL.AcquireLease(context.Background(), leaseID, leaseDuration, azblob.ModifiedAccessConditions{})
	if err != nil {
		telemetry.TrackTrace("PartitionMgr::AcquireLease::Error acquiring lease", telemetry.Error, map[string]string{"Error": err.Error()})
		return "", err
	}

	// Lease acquired successfully
	return partitionID, nil
}

// handleError logs the error message and error to App Insights
func handleError(message string, err error) {
	// Log the error using telemetry
	fmt.Println("PartitionManager::handleError::Message: ", message)
	fmt.Println("PartitionManager::handleError::Error: ", err)
	telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": message})
}
