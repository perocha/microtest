package storage

import (
	"context"
	"errors"
	"net/url"
	"strconv"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type BlobStorage struct {
	ContainerURL azblob.ContainerURL
}

type LeaseManager struct {
	containerURL azblob.ContainerURL
}

// Creates a new BlobStorage instance
func NewBlobStorage(accountName, accountKey, containerName string) (*BlobStorage, error) {
	// Create Blob Storage client
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
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
		return "", err
	}
	bodyStream := resp.Body(azblob.RetryReaderOptions{})
	defer bodyStream.Close()

	// Read checkpoint from response body
	data := make([]byte, resp.ContentLength())
	_, err = bodyStream.Read(data)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func NewLeaseManager(accountName, accountKey, containerName string) (*LeaseManager, error) {
	// Create Blob Storage client
	blobStorage, err := NewBlobStorage(accountName, accountKey, containerName)
	if err != nil {
		return nil, err
	}

	return &LeaseManager{containerURL: blobStorage.ContainerURL}, nil
}

func (lm *LeaseManager) AcquireLease(consumerID, numPartitions int, leaseDuration int32) (string, error) {
	// Create lease ID based on consumer ID
	leaseID := "consumer-" + strconv.Itoa(consumerID)

	// Try to acquire a lease for each partition until success
	for i := 0; i < numPartitions; i++ {
		partitionID := strconv.Itoa(i)
		blobURL := lm.containerURL.NewBlockBlobURL(partitionID)

		// Try to acquire the lease
		_, err := blobURL.AcquireLease(context.Background(), leaseID, leaseDuration, azblob.ModifiedAccessConditions{})
		if err == nil {
			// Lease acquired successfully
			return partitionID, nil
		}
	}

	// If no lease can be acquired, return an error
	return "", errors.New("failed to acquire lease for any partition")
}
