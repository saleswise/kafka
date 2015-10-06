package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/saleswise/kazoo-go"
)

var (
	AlreadyClosing = errors.New("The consumer group is already shutting down.")
)

type Config struct {
	*sarama.Config

	Zookeeper *kazoo.Config

	Offsets struct {
		Initial           int64         // The initial offset method to use if the consumer has no previously stored offset. Must be either sarama.OffsetOldest (default) or sarama.OffsetNewest.
		ProcessingTimeout time.Duration // Time to wait for all the offsets for a partition to be processed after stopping to consume from it. Defaults to 1 minute.
		CommitInterval    time.Duration // The interval between which the prossed offsets are commited.
	}
}

func NewConfig() *Config {
	config := &Config{}
	config.Config = sarama.NewConfig()
	config.Zookeeper = kazoo.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 60 * time.Second
	config.Offsets.CommitInterval = 10 * time.Second

	return config
}

func (cgc *Config) Validate() error {
	if cgc.Zookeeper.Timeout <= 0 {
		return sarama.ConfigurationError("ZookeeperTimeout should have a duration > 0")
	}

	if cgc.Offsets.CommitInterval <= 0 {
		return sarama.ConfigurationError("CommitInterval should have a duration > 0")
	}

	if cgc.Offsets.Initial != sarama.OffsetOldest && cgc.Offsets.Initial != sarama.OffsetNewest {
		return errors.New("Offsets.Initial should be sarama.OffsetOldest or sarama.OffsetNewest.")
	}

	if cgc.Config != nil {
		if err := cgc.Config.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// The ConsumerGroup type holds all the information for a consumer that is part
// of a consumer group. Call JoinConsumerGroup to start a consumer.
type ConsumerGroup struct {
	config *Config

	consumer sarama.Consumer
	kazoo    *kazoo.Kazoo
	group    *kazoo.Consumergroup
	instance *kazoo.ConsumergroupInstance

	wg             sync.WaitGroup
	singleShutdown sync.Once

	// messages is a map of topics to message slices, one per stream per topic.
	messages map[string][]chan *sarama.ConsumerMessage

	errors  chan *sarama.ConsumerError
	stopper chan struct{}

	consumers kazoo.ConsumergroupInstanceList

	offsetManager OffsetManager
	manyChannels  bool

	myPartitions map[int32]bool
}

// Connects to a consumer group, using Zookeeper for auto-discovery
func JoinConsumerGroup(name string, topics []string, zookeeper []string, config *Config) (cg *ConsumerGroup, err error) {
	chanList := make([]chan *sarama.ConsumerMessage, 1, 1)
	bufSz := 0
	if config != nil {
		bufSz = config.ChannelBufferSize
	}
	chanList[0] = make(chan *sarama.ConsumerMessage, bufSz)
	topicStreamMap := make(map[string][]chan *sarama.ConsumerMessage)
	for _, topic := range topics {
		topicStreamMap[topic] = chanList
	}

	cg, _, err = joinConsumerGroupWithStreams(name, topics, topicStreamMap, zookeeper, config)
	if err != nil {
		return nil, err
	}
	cg.manyChannels = false
	return cg, err
}

// Connects to a consumer group, using Zookeeper for auto-discovery, and create a set of streams to topics as per to the provided stream map.
func JoinConsumerGroupWithStreams(name string, topicStreams map[string]int, zookeeper []string, config *Config) (cg *ConsumerGroup, streams map[string][]<-chan *sarama.ConsumerMessage, err error) {
	messages := make(map[string][]chan *sarama.ConsumerMessage)
	topics := make([]string, 0)

	bufSz := 0
	if config != nil {
		bufSz = config.ChannelBufferSize
	}

	// Create message channels per topic.
	for topic, messageCount := range topicStreams {
		topics = append(topics, topic)
		messages[topic] = make([]chan *sarama.ConsumerMessage, messageCount, messageCount)

		// Create a messages channel per stream
		for i := 0; i < topicStreams[topic]; i++ {
			messages[topic][i] = make(chan *sarama.ConsumerMessage, bufSz)
		}
	}

	cg, s, err := joinConsumerGroupWithStreams(name, topics, messages, zookeeper, config)
	if err != nil {
		return nil, nil, err
	}
	cg.manyChannels = true
	return cg, s, err
}

func joinConsumerGroupWithStreams(name string, topics []string, topicStreams map[string][]chan *sarama.ConsumerMessage, zookeeper []string, config *Config) (cg *ConsumerGroup, streams map[string][]<-chan *sarama.ConsumerMessage, err error) {

	if name == "" {
		return nil, nil, sarama.ConfigurationError("Empty consumergroup name")
	}

	if len(topics) == 0 {
		return nil, nil, sarama.ConfigurationError("No topics provided")
	}

	if len(zookeeper) == 0 {
		return nil, nil, errors.New("You need to provide at least one zookeeper node address!")
	}

	for _, streams := range topicStreams {
		if len(streams) < 1 {
			return nil, nil, errors.New("You need to have at minimum 1 stream per topic")
		}
	}

	if config == nil {
		config = NewConfig()
	}
	config.ClientID = name

	// Validate configuration
	if err = config.Validate(); err != nil {
		return
	}

	var kz *kazoo.Kazoo
	if kz, err = kazoo.NewKazoo(zookeeper, config.Zookeeper); err != nil {
		return
	}

	brokers, err := kz.BrokerList()
	if err != nil {
		kz.Close()
		return
	}

	group := kz.Consumergroup(name)
	instance := group.NewInstance()

	var consumer sarama.Consumer
	if consumer, err = sarama.NewConsumer(brokers, config.Config); err != nil {
		kz.Close()
		return
	}

	cg = &ConsumerGroup{
		config:   config,
		consumer: consumer,

		kazoo:    kz,
		group:    group,
		instance: instance,

		messages: topicStreams,
		errors:   make(chan *sarama.ConsumerError, config.ChannelBufferSize),
		stopper:  make(chan struct{}),
	}

	streams = make(map[string][]<-chan *sarama.ConsumerMessage)
	for t, l := range topicStreams {
		streams[t] = make([]<-chan *sarama.ConsumerMessage, len(l), len(l))
		for i, c := range l {
			streams[t][i] = c
		}
	}

	// Register consumer group
	if exists, err := cg.group.Exists(); err != nil {
		cg.Logf("FAILED to check for existence of consumergroup: %s!\n", err)
		_ = consumer.Close()
		_ = kz.Close()
		return nil, nil, err
	} else if !exists {
		cg.Logf("Consumergroup `%s` does not yet exists, creating...\n", cg.group.Name)
		if err := cg.group.Create(); err != nil {
			cg.Logf("FAILED to create consumergroup in Zookeeper: %s!\n", err)
			_ = consumer.Close()
			_ = kz.Close()
			return nil, nil, err
		}
	}

	// Register itself with zookeeper
	if err := cg.instance.Register(topics); err != nil {
		cg.Logf("FAILED to register consumer instance: %s!\n", err)
		return nil, nil, err
	} else {
		cg.Logf("Consumer instance registered (%s).", cg.instance.ID)
	}

	offsetConfig := OffsetManagerConfig{CommitInterval: config.Offsets.CommitInterval}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig)

	go cg.topicListConsumer(topics)

	return
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Messages() <-chan *sarama.ConsumerMessage {
	for _, topic := range cg.messages {
		return topic[0]
	}

	return nil
}

func (cg *ConsumerGroup) Stream(topic string, i int) (<-chan *sarama.ConsumerMessage, error) {
	topicMessages, ok := cg.messages[topic]
	if !ok {
		return nil, errors.New("Requested topic is not registered against this consumergroup")
	}

	if i >= len(topicMessages) || i < 0 {
		return nil, errors.New("Requested stream number is out-of-bounds")
	}

	return topicMessages[i], nil
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Errors() <-chan *sarama.ConsumerError {
	return cg.errors
}

func (cg *ConsumerGroup) Closed() bool {
	return cg.instance == nil
}

func (cg *ConsumerGroup) Close() error {
	shutdownError := AlreadyClosing
	cg.singleShutdown.Do(func() {
		defer cg.kazoo.Close()

		shutdownError = nil

		close(cg.stopper)
		cg.wg.Wait()

		if err := cg.offsetManager.Close(); err != nil {
			cg.Logf("FAILED closing the offset manager: %s!\n", err)
		}

		if shutdownError = cg.instance.Deregister(); shutdownError != nil {
			cg.Logf("FAILED deregistering consumer instance: %s!\n", shutdownError)
		} else {
			cg.Logf("Deregistered consumer instance %s.\n", cg.instance.ID)
		}

		if shutdownError = cg.consumer.Close(); shutdownError != nil {
			cg.Logf("FAILED closing the Sarama client: %s\n", shutdownError)
		}

	CloseChannels:
		for _, messages := range cg.messages {
			for _, messageCh := range messages {
				close(messageCh)
				if !cg.manyChannels {
					break CloseChannels
				}
			}
		}
		close(cg.errors)
		cg.instance = nil
	})

	return shutdownError
}

func (cg *ConsumerGroup) Logf(format string, args ...interface{}) {
	var identifier string
	if cg.instance == nil {
		identifier = "(defunct)"
	} else {
		identifier = cg.instance.ID[len(cg.instance.ID)-12:]
	}
	sarama.Logger.Printf("[%s/%s] %s", cg.group.Name, identifier, fmt.Sprintf(format, args...))
}

func (cg *ConsumerGroup) InstanceRegistered() (bool, error) {
	return cg.instance.Registered()
}

func (cg *ConsumerGroup) CommitUpto(message *sarama.ConsumerMessage) error {
	_, err := cg.offsetManager.MarkAsProcessed(message.Topic, message.Partition, message.Offset)
	return err
}

func (cg *ConsumerGroup) topicListConsumer(topics []string) {
	writeOnlyMessages := make(map[string][]chan<- *sarama.ConsumerMessage)
	for v, l := range cg.messages {
		writeOnlyMessages[v] = make([]chan<- *sarama.ConsumerMessage, len(l), len(l))
		for i, ch := range l {
			writeOnlyMessages[v][i] = ch
		}
	}

	for {
		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.group.WatchInstances()
		if err != nil {
			cg.Logf("FAILED to get list of registered consumer instances: %s\n", err)
			cg.errors <- &sarama.ConsumerError{
				Topic:     "",
				Partition: -1,
				Err:       err,
			}
			return
		}

		cg.consumers = consumers
		cg.Logf("Currently registered consumers: %d\n", len(cg.consumers))

		stopper := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(1)
			go cg.topicConsumer(topic, writeOnlyMessages[topic], cg.errors, stopper)
		}

		select {
		case <-cg.stopper:
			close(stopper)
			return

		case <-consumerChanges:
			cg.Logf("Triggering rebalance due to consumer list change, waiting for shutdown\n")
			close(stopper)
			cg.wg.Wait()
			cg.Logf("Done waiting, continuing with rebalance\n")
		}
	}
}

func (cg *ConsumerGroup) topicConsumer(topic string, messages []chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, stopper <-chan struct{}) {
	defer cg.wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	cg.Logf("%s :: Started topic consumer\n", topic)

	// Fetch a list of partition IDs
	partitions, err := cg.kazoo.Topic(topic).Partitions()
	if err != nil {
		cg.Logf("%s :: FAILED to get list of partitions: %s\n", topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	partitionLeaders, err := retrievePartitionLeaders(partitions)
	if err != nil {
		cg.Logf("%s :: FAILED to get leaders of partitions: %s\n", topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitionLeaders)
	myPartitions := dividedPartitions[cg.instance.ID]

	cg.Logf("%s :: Claiming %d of %d partitions", topic, len(myPartitions), len(partitionLeaders))
	var myPIDs []int32
	for _, p := range myPartitions {
		myPIDs = append(myPIDs, p.ID)
	}
	cg.Logf("%s :: My partition IDs: %v", topic, myPIDs)

	partitionStreamAssignments := dividePbetweenC(len(myPartitions), len(messages))

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for i, pid := range myPartitions {

		wg.Add(1)
		streamMessages := messages[partitionStreamAssignments[i]]
		go cg.partitionConsumer(topic, pid.ID, streamMessages, errors, &wg, stopper)
	}

	myPartitionsMap := make(map[int32]bool)
	for _, p := range myPartitions {
		myPartitionsMap[p.ID] = true
	}
	cg.myPartitions = myPartitionsMap

	wg.Wait()
	cg.Logf("%s :: Stopped topic consumer\n", topic)
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, messages chan<- *sarama.ConsumerMessage, errors chan<- *sarama.ConsumerError, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()
	defer cg.Logf("%s/%d :: Returning from partitionConsumer", topic, partition)

	select {
	case <-stopper:
		return
	default:
	}

	// Backoff to handle race condition. Must wait more than the 60s wait time from OffsetManager.FinalizePartition
	// otherwise this would give up and that partition would no longer be claimed by any consumer.
	for maxRetries, tries := 75, 0; tries < maxRetries; tries++ {
		if err := cg.instance.ClaimPartition(topic, partition); err == nil {
			break
		} else if err == kazoo.ErrPartitionClaimedByOther && tries+1 < maxRetries {
			cg.Logf("%s/%d :: Backing off claim to the partition.\n", topic, partition)
			time.Sleep(1 * time.Second)
		} else {
			errors <- &sarama.ConsumerError{
				Topic:     topic,
				Partition: partition,
				Err:       err,
			}
			cg.Logf("%s/%d :: FAILED to claim the partition: %s\n", topic, partition, err)
			return
		}
	}

	defer cg.instance.ReleasePartition(topic, partition)

	nextOffset, err := cg.offsetManager.InitializePartition(topic, partition)
	if err != nil {
		errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
		cg.Logf("%s/%d :: FAILED to determine initial offset: %s\n", topic, partition, err)
		return
	}

	if nextOffset >= 0 {
		cg.Logf("%s/%d :: Partition consumer starting at offset %d.\n", topic, partition, nextOffset)
	} else {
		nextOffset = cg.config.Offsets.Initial
		if nextOffset == sarama.OffsetOldest {
			cg.Logf("%s/%d :: Partition consumer starting at the oldest available offset.\n", topic, partition)
		} else if nextOffset == sarama.OffsetNewest {
			cg.Logf("%s/%d :: Partition consumer listening for new messages only.\n", topic, partition)
		}
	}

	consumer, err := cg.consumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		cg.Logf("%s/%d :: Partition consumer offset out of Range.\n", topic, partition)
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if cg.config.Offsets.Initial == sarama.OffsetOldest {
			nextOffset = sarama.OffsetOldest
			cg.Logf("%s/%d :: Partition consumer offset reset to oldest available offset.\n", topic, partition)
		} else {
			nextOffset = sarama.OffsetNewest
			cg.Logf("%s/%d :: Partition consumer offset reset to newest available offset.\n", topic, partition)
		}
		// retry the consumePartition with the adjusted offset
		consumer, err = cg.consumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
		errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
		cg.Logf("%s/%d :: FAILED to start partition consumer: %s\n", topic, partition, err)
		return
	}
	defer cg.Logf("%s/%d :: Closed sarama consumer", topic, partition)
	defer consumer.Close()

	err = nil
	var lastOffset int64 = -1 // aka unknown
partitionConsumerLoop:
	for {
		select {
		case <-stopper:
			break partitionConsumerLoop

		case err := <-consumer.Errors():
			for {
				select {
				case errors <- err:
					continue partitionConsumerLoop

				case <-stopper:
					break partitionConsumerLoop
				}
			}

		case message := <-consumer.Messages():
			for {
				select {
				case <-stopper:
					break partitionConsumerLoop

				case messages <- message:
					lastOffset = message.Offset
					continue partitionConsumerLoop
				}
			}
		}
	}

	cg.Logf("%s/%d :: Stopping partition consumer at offset %d\n", topic, partition, lastOffset)
	if err := cg.offsetManager.FinalizePartition(topic, partition, lastOffset, cg.config.Offsets.ProcessingTimeout); err != nil {
		cg.Logf("%s/%d :: %s\n", topic, partition, err)
		errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
	}
}

func (cg *ConsumerGroup) IsMyPartition(id int32) bool {
	return cg.myPartitions[id]
}
