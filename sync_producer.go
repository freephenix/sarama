package sarama

import "sync"

// SyncProducer publishes Kafka messages, blocking until they have been acknowledged. It routes messages to the correct
// broker, refreshing metadata as appropriate, and parses responses for errors. You must call Close() on a producer
// to avoid leaks, it may not be garbage-collected automatically when it passes out of scope.
//
// The SyncProducer comes with two caveats: it will generally be less efficient than the AsyncProducer, and the actual
// durability guarantee provided when a message is acknowledged depend on the configured value of `Producer.RequiredAcks`.
// There are configurations where a message acknowledged by the SyncProducer can still sometimes be lost.
//
// For implementation reasons, the SyncProducer requires `Producer.Return.Errors` and `Producer.Return.Successes` to
// be set to true in its configuration.
// SyncProducer 用于发送kafka消息，在收到确认回复前会一直阻塞。它负责将消息发送到正确的broker，
// 适当刷新元数据，并且会解析错误返回。当逃逸时，它并不一定会被垃圾回收自动清理掉，所以为了防止资源泄露，你需要调用 Close()。
// 使用 SyncProducer 有两个注意点：它通常比 AsyncProducer 效率低，并且在确认消息时提供的实际持久性保证取决于
// Producer.RequiredAcks 的配置值，在某些配置中，由 SyncProducer 确认的消息有时仍会丢失。
// 因为实现的原因， SyncProducer 需要`Producer.Return.Errors` and `Producer.Return.Successes`两个配置项设置为true
type SyncProducer interface {

	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	// SendMessage 发送消息接口
	SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error)

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	// SendMessage 批量发送消息接口，有一个错误就会返回error
	SendMessages(msgs []*ProducerMessage) error

	// Close shuts down the producer and waits for any buffered messages to be
	// flushed. You must call this function before a producer object passes out of
	// scope, as it may otherwise leak memory. You must call this before calling
	// Close on the underlying client.
	// Close 清理资源接口，记得一定要调用啊，否则会造成资源泄露
	Close() error
}

type syncProducer struct {
	producer *asyncProducer
	wg       sync.WaitGroup
}

// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
// NewSyncProducer 创建一个新的同步发送者
func NewSyncProducer(addrs []string, config *Config) (SyncProducer, error) {
	if config == nil {
		config = NewConfig()
		config.Producer.Return.Successes = true
	}

	if err := verifyProducerConfig(config); err != nil {
		return nil, err
	}

	// 其实还是起一个异步发送者
	p, err := NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	// 异步接受成功和错误信息，应该是通过阻塞返回值实现的了
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

// NewSyncProducerFromClient creates a new SyncProducer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewSyncProducerFromClient(client Client) (SyncProducer, error) {
	if err := verifyProducerConfig(client.Config()); err != nil {
		return nil, err
	}

	p, err := NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return newSyncProducerFromAsyncProducer(p.(*asyncProducer)), nil
}

func newSyncProducerFromAsyncProducer(p *asyncProducer) *syncProducer {
	sp := &syncProducer{producer: p}

	sp.wg.Add(2)
	go withRecover(sp.handleSuccesses)
	go withRecover(sp.handleErrors)

	return sp
}

// verifyProducerConfig 同步发送者 `config.Producer.Return.Errors` 和 `config.Producer.Return.Successes`
// 两个配置必须是要为true
func verifyProducerConfig(config *Config) error {
	if !config.Producer.Return.Errors {
		return ConfigurationError("Producer.Return.Errors must be true to be used in a SyncProducer")
	}
	if !config.Producer.Return.Successes {
		return ConfigurationError("Producer.Return.Successes must be true to be used in a SyncProducer")
	}
	return nil
}

func (sp *syncProducer) SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error) {
	expectation := make(chan *ProducerError, 1)
	msg.expectation = expectation
	sp.producer.Input() <- msg

	if err := <-expectation; err != nil {
		return -1, -1, err.Err
	}

	return msg.Partition, msg.Offset, nil
}

func (sp *syncProducer) SendMessages(msgs []*ProducerMessage) error {
	expectations := make(chan chan *ProducerError, len(msgs))
	go func() {
		for _, msg := range msgs {
			expectation := make(chan *ProducerError, 1)
			msg.expectation = expectation
			sp.producer.Input() <- msg
			expectations <- expectation
		}
		close(expectations)
	}()

	var errors ProducerErrors
	for expectation := range expectations {
		if err := <-expectation; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (sp *syncProducer) handleSuccesses() {
	defer sp.wg.Done()
	for msg := range sp.producer.Successes() {
		expectation := msg.expectation
		expectation <- nil
	}
}

func (sp *syncProducer) handleErrors() {
	defer sp.wg.Done()
	for err := range sp.producer.Errors() {
		expectation := err.Msg.expectation
		expectation <- err
	}
}

func (sp *syncProducer) Close() error {
	sp.producer.AsyncClose()
	sp.wg.Wait()
	return nil
}
