# batcher

Tiny batcher for GraphQL Dataloader or Batch user request.

## basic settings

- `MaxBatchSize` - the maximum size of data that is passed for processing to a user-defined function (`handler`).
- `MinBatchSize` - the minimul size of data that is passed for processing to `handler`.
- `MaxHandlers` - the maximum number of threads (goroutines) that are used for processing `handler`.
- `Timeout` - timeout for context to process `handler`. If not set explicitly, then the context of the first key from the batch is used. *We strongly recommend* using this option to limit the execution time of `handler`
- `Ticker` - the interval for starting an additional batch handler that collects the rest of the pending data. Helps to process data stuck in the queue faster. _Default: 17ms_
- `WithCache` - setting data caching from `handler`

## how it works

1. The new key is placed in the public pool (`b.pending`) and use `runBatch`
1. The `runBatch` checks the number of already running handlers. If this number >= the maximum set, then we leave the processing of this key to already running handlers and return the channel for waiting for the result.
1. If the number of handlers < the maximum set, we try to get a batch of data for processing. If batch length > 0 we start a new handler.
1. The handler generates a slice of keys from the batch, and launches the user handler. 
1. Аfter processing the user function, the handler tries to get a new batch of pending data and performs processing or completes its execution

### Timer batch

When initialized, the batcher launches a ticker, which every time interval (default 17ms) tries to get the rest of the pending data and process it. 
This is necessary to ensure that all data that is put in the waiting queue will be sent for processing with minimal letansi from the batcher.

For example:
> If we set the minimum batch size to 20 elements, and we receive 19 elements, and the next element will arrive after 200ms. In this way, a normal batcher will expect all 20 elements to be grouped into 1 request.

To reduce latency in this situation, we just launch a new batch every 17ms.

On the first pass, such a batcher picks up all pending records where 0 < n < the maximum batch size.

After the first pass is completed and until a new data patch is received, the batcher changes its type to the `MainBatcher` and then will work like a regular batcher that waits for a full batch of data or ends.
This is correct to ensure that we do not have a bunch of batchers that will fetch the amount of data less than the minimum bucket size.