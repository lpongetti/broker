package broker

import (
	"context"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sqsextendedclient "github.com/co-go/sqs-extended-client-go"
	"golang.org/x/sync/errgroup"
)

type sqsBroker struct {
	config *AwsConfig
	sqsSvc *sqsextendedclient.Client
}

type AwsConfig struct {
	Region    string
	AccessKey string
	SecretKey string
	S3Bucket  string
}

func NewSqs(cfg *AwsConfig) IBroker {
	awsCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
		config.WithRegion(cfg.Region),
	)
	if err != nil {
		panic(err)
	}

	sqsSvc, _ := sqsextendedclient.New(
		sqs.NewFromConfig(awsCfg),
		s3.NewFromConfig(awsCfg),
		sqsextendedclient.WithS3BucketName(cfg.S3Bucket),
	)

	return &sqsBroker{
		config: cfg,
		sqsSvc: sqsSvc,
	}
}

func (r *sqsBroker) Publish(ctx context.Context, queue string, groupId string, data *string) error {
	_, err := r.sqsSvc.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       &queue,
		MessageGroupId: &groupId,
		MessageBody:    data,
	})
	return err
}

func (r *sqsBroker) Subscribe(ctx context.Context, conf Configuration, fn func(context.Context, IMessage)) error {
	var messageCount int32 = 0
	message := make(chan IMessage, conf.MaxMessages)
	errSub := make(chan error, conf.MaxMessages)

	g, gCtx := errgroup.WithContext(ctx)

	go func() {
		for {
			select {
			case <-gCtx.Done():
				for i := 0; i < conf.MaxMessages; i++ {
					errSub <- nil
				}
				return
			default:

				if messageCount == int32(conf.MaxMessages) {
					continue
				}
				output, err := r.sqsSvc.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
					QueueUrl:          aws.String(conf.Queue),
					WaitTimeSeconds:   15,
					VisibilityTimeout: 60,
					AttributeNames:    []types.QueueAttributeName{"MessageGroupId"},
				})
				if err != nil {
					errSub <- err
					return
				}

				for index := range output.Messages {
					msg := output.Messages[index]

					atomic.AddInt32(&messageCount, 1)
					message <- NewMessage(
						[]byte(*msg.Body),
						func() error {
							return r.deleteMessage(conf.Queue, msg)
						},
						msg.Attributes["MessageGroupId"],
						func(vctx context.Context, timeout int32) error {
							return r.changeVisibility(vctx, conf.Queue, msg, timeout)
						},
						errSub,
					)
				}
			}
		}
	}()

	for i := 0; i < conf.MaxMessages; i++ {
		g.Go(func() error {
			for {
				select {
				case msg := <-message:
					fn(gCtx, msg)
					atomic.AddInt32(&messageCount, -1)
				case err := <-errSub:
					return err
				}
			}
		})
	}

	return g.Wait()
}

func (r *sqsBroker) deleteMessage(queue string, msg types.Message) error {
	_, err := r.sqsSvc.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queue),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

func (r *sqsBroker) changeVisibility(ctx context.Context, queue string, msg types.Message, timeout int32) error {
	_, err := r.sqsSvc.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queue),
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: timeout,
	})
	return err
}
