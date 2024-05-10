package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/joho/godotenv"
)

func getClient() (*azblob.Client, error) {
	accountKey := os.Getenv("ENV_VAR")
	accountName := "bwsdev01sa"

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	handleError(err)

	url := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	return azblob.NewClientWithSharedKeyCredential(url, cred, nil)
}

func Upload(fileName string, containerName string, new_file_name string, retry int) error {
	if retry == 5 {
		errorLog, err := os.OpenFile("log_error.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer errorLog.Close()

		// Ghi dòng thông báo vào error_log.txt
		if _, err := errorLog.WriteString(fmt.Sprintf("Failed to upload file %s after 5 retries\n", fileName)); err != nil {
			return err
		}

		// Trả về lỗi sau khi ghi vào error_log.txt
		return nil
	}

	client, err := getClient()
	if err != nil {
		return err
	}

	file, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	// _, err = client.UploadFile(context.Background(), containerName, new_file_name, file, nil)
	_, err = client.UploadFile(context.Background(), containerName, new_file_name, nil, nil)

	if err != nil {
		return Upload(fileName, containerName, new_file_name, retry+1)
	}
	return err
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

type FileName struct {
	path          string
	new_file_name string
}

type WorkerPool struct {
	workerCount int
	jobs        chan FileName
	wg          sync.WaitGroup
}

func NewWorkerPool(workerCount int) *WorkerPool {
	return &WorkerPool{
		workerCount: workerCount,
		jobs:        make(chan FileName),
	}
}

func (wp *WorkerPool) AddJob(job FileName) {
	wp.jobs <- job
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		go wp.worker()
	}
}

func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

func (wp *WorkerPool) worker() {
	wp.wg.Add(1)
	for job := range wp.jobs {
		// Đọc tệp hình ảnh và gửi lên Azure Storage
		err := processImage(job)
		if err != nil {
			fmt.Printf("Error processing image %s: %v", job.new_file_name, err)
		}
	}
	fmt.Printf("Done\n")
	wp.wg.Done()
}

func processImage(fileJob FileName) error {
	// Đọc và xử lý tệp hình ảnh
	// Code xử lý ảnh ở đây
	fmt.Printf("Processing image: %s\n", fileJob.path)
	// Gửi ảnh lên Azure Storage
	// Code gửi lên Azure Storage ở đây
	file, err := os.Open(fileJob.path)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	containerName := "categories-dev"
	new_file_name := fileJob.new_file_name
	// Tải lên tệp
	errUpload := Upload(fileJob.path, containerName, new_file_name, 0)
	handleError(errUpload)
	// In ra kích thước của tệp tin
	return nil
}

// Hàm đệ qui để đọc tất cả các tệp trong một thư mục và thư mục con
func readDirectoryRecursive(directoryPath string, wp *WorkerPool, count *int) error {
	// Mở thư mục
	dir, err := os.Open(directoryPath)
	if err != nil {
		return err
	}
	defer fmt.Print("----------------End---------------\n")
	defer dir.Close()
	defer close(wp.jobs)


	// Đọc tất cả các mục trong thư mục
	items, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	// Lặp qua từng mục
	for _, item := range items {
		// Xây dựng đường dẫn đầy đủ của mục
		fullPath := filepath.Join(directoryPath, item.Name())

		// Kiểm tra nếu mục là thư mục
		if item.IsDir() {
			// Nếu là thư mục, đọc tiếp bên trong
			fmt.Println("Directory:", fullPath)
			if err := readDirectoryRecursive(fullPath, wp, count); err != nil {
				return err
			}
		} else {
			// Nếu là tệp tin, xử lý tệp tin
			*count = *count + 1
			fmt.Println("Count:", *count)
			// Thực hiện các xử lý mong muốn với tệp tin ở đây
			fileJob := FileName{fullPath, fullPath}
			wp.AddJob(fileJob)
		}
	}

	return nil
}

func main() {
	fmt.Printf("Azure Blob storage quick start\n")

	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	start := time.Now()

	directoryPath := "BACKUP_HINHANH/"

	wp := NewWorkerPool(8)
	// Khởi động worker pool
	wp.Start()
	count := 0
	// Gọi hàm để đọc thư mục
	if err := readDirectoryRecursive(directoryPath, wp, &count); err != nil {
		fmt.Println("Error:", err)
	}

	wp.Wait()

	fmt.Println("All jobs completed")
	fmt.Println("count: ", count)
	duration := time.Since(start)
	fmt.Println("Thời gian chạy:", duration)

	// pager := client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
	// 	Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	// })

	// for pager.More() {
	// 	resp, err := pager.NextPage(context.TODO())
	// 	handleError(err)

	// 	for _, blob := range resp.Segment.BlobItems {
	// 		fmt.Println(*blob.Name)
	// 	}
	// }
}
