# Pipeline System Architecture

## Overview

The pipeline system provides a composable architecture for processing forkable data streams, designed specifically for blockchain indexing and similar applications where data can branch and require rollback capabilities.

## Core Concepts

### Data Model

All data flows through the system as typed batches with references for ordering and fork detection:

```typescript
// Base data interface
export interface Data<I = unknown, R = unknown> {
    item: I    // The actual data content
    ref: R     // Reference for ordering/comparison
}

// Helper types for extracting item and reference types
export type DataItem<D extends Data> = D['item']
export type DataRef<D extends Data> = D['ref']

// Batch of data items with metadata
export interface DataBatch<T extends Data> {
    readonly data: DataItem<T>[]           // Array of data items
    readonly finalizedHead?: DataRef<T>    // Last finalized reference
    readonly head: DataRef<T>              // Current head reference
    readonly offset: DataRef<T>            // Current offset reference
}

// Fork information when divergence is detected
export interface DataFork<T extends Data> {
    readonly heads: DataRef<T>[]           // All competing head references
}

// Reference comparison interface
export interface DataCursor<T extends Data> {
    get(ref: DataItem<T>): DataRef<T>                           // Get ref from item
    compare(a: DataRef<T>, b: DataRef<T>): 'ls' | 'eq' | 'gt' | 'fk'  // Compare refs
}
```

### Finalized vs Unfinalized System

The system distinguishes between finalized and unfinalized data processing:

- **Finalized (unfinalized: false)**: Can handle forks internally without throwing exceptions
- **Unfinalized (unfinalized: true)**: Throws `ForkException` when forks are detected, requiring external handling

## Core Interfaces

### Data Source Types

```typescript
// Finalized data source - can pipe to any target
export interface FinalizedDataSource<T extends Data> {
    readonly unfinalized: false
    readonly cursor: DataCursor<T>
    read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: {target: DataTarget<T>; source: U}): U
    pipeTo(target: DataTarget<T>): Promise<void>
    close(): Promise<void>
}

// Unfinalized data source - can only pipe to unfinalized targets
export interface UnfinalizedDataSource<T extends Data> {
    readonly unfinalized: true
    readonly cursor: DataCursor<T>
    read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: {target: UnfinalizedDataTarget<T>; source: U}): U
    pipeTo(target: UnfinalizedDataTarget<T>): Promise<void>
    close(): Promise<void>
}

// Union type
export type DataSource<T extends Data> = FinalizedDataSource<T> | UnfinalizedDataSource<T>

// Data reader options
export interface DataReaderOptions<T extends Data> {
    offset: DataRef<T> | undefined
}
```

### Data Target Types

```typescript
// Finalized target - has optional fork handling  
export interface FinalizedDataTarget<T extends Data> {
    unfinalized: false
    write(opts: DataWriterOptions<T>, read: (opts: DataReaderOptions<T>) => AsyncIterable<DataBatch<T>>): Promise<void>
    close(): Promise<void>
}

// Unfinalized target - requires fork handling
export interface UnfinalizedDataTarget<T extends Data> {
    unfinalized: true
    write(opts: DataWriterOptions<T>, read: (opts: DataReaderOptions<T>) => AsyncIterable<DataBatch<T>>): Promise<void>
    close(): Promise<void>
}

// Union type
export type DataTarget<T extends Data> = UnfinalizedDataTarget<T> | FinalizedDataTarget<T>

// Data writer options
export interface DataWriterOptions<T extends Data> {
    cursor: DataRef<T>
}

// Data duplex - combines target and source for transformations
export interface DataDuplex<T extends Data, U extends Data> {
    target: DataTarget<T>                                         // Input target
    source: DataSource<U>                                         // Output source
}
```

## Reader and Writer Interfaces

```typescript
// Data reader interface - provides data with lifecycle management
export interface DataReader<T extends Data> {
    read(): Promise<DataBatch<T> | undefined>
    close?(): Promise<unknown>
}

// Base data writer interface
export interface FinalizedDataWriter<T extends Data> {
    offset: DataRef<T> | undefined
    write(batch: DataBatch<T>): Promise<unknown>
    fork?(fork: DataFork<T>): Promise<DataRef<T> | undefined>
    close?(): Promise<unknown>
}

// Unfinalized writer - requires fork handling
export interface UnfinalizedDataWriter<T extends Data> extends FinalizedDataWriter<T> {
    fork(fork: DataFork<T>): Promise<DataRef<T> | undefined>
}

// Union type
export type DataWriter<T extends Data> = FinalizedDataWriter<T> | UnfinalizedDataWriter<T>
```

## Factory Functions

```typescript
// Finalized data source config
export interface FinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    cursor: DataCursor<T>
    unfinalized?: false
}

// Unfinalized data source config
export interface UnfinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    cursor: DataCursor<T>
    unfinalized: true
}

// Create unfinalized data source
export function source<T extends Data>(config: UnfinalizedDataSourceConfig<T>): UnfinalizedDataSource<T>

// Create finalized data source
export function source<T extends Data>(config: FinalizedDataSourceConfig<T>): FinalizedDataSource<T>

// Finalized data target config
export interface FinalizedDataTargetConfig<T extends Data> {
    writer: () => PromiseLike<DataWriter<T>>
    unfinalized?: false
}

// Unfinalized data target config
export interface UnfinalizedDataTargetConfig<T extends Data> {
    writer: () => PromiseLike<UnfinalizedDataWriter<T>>
    unfinalized: true
}

// Create unfinalized data target
export function target<T extends Data>(config: UnfinalizedDataTargetConfig<T>): UnfinalizedDataTarget<T>

// Create finalized data target
export function target<T extends Data>(config: FinalizedDataTargetConfig<T>): FinalizedDataTarget<T>
```

## Pipeline Operations

```typescript
// Execute pipeline with automatic fork handling
export async function pipe<T extends Data>(
    source: DataSource<T>, 
    target: DataTarget<T>
): Promise<void>
```

<!-- Transformer functionality is not yet implemented
```typescript
// Transform data batches - unfinalized version
export interface UnfinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork: (fork: DataFork<T>, cursor: DataCursor<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    cursor: DataCursor<U>
    unfinalized: true
}

// Transform data batches - finalized version
export interface FinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork?: (fork: DataFork<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    cursor: DataCursor<U>
    unfinalized: false
}

export function transformer<T extends Data, U extends Data>(
    config: UnfinalizedTransformerConfig<T, U>
): {target: UnfinalizedDataTarget<T>; source: UnfinalizedDataSource<U>}

export function transformer<T extends Data, U extends Data>(
    config: FinalizedTransformerConfig<T, U>
): {target: FinalizedDataTarget<T>; source: FinalizedDataSource<U>}
```
-->

## Fork Exception Handling

```typescript
export class ForkException<D extends Data> extends Error {
    readonly isSqdForkException = true
    readonly fork: DataFork<D>
    
    constructor(fork: DataFork<D>)
}

export const isForkException = <D extends Data>(err: unknown): err is ForkException<D>
```

## Usage Patterns

### Basic Pipeline Construction

```typescript
// Create unfinalized source with reader factory
const dataSource = source({
  reader: (opts) => createReader(opts),
  cursor: blockCursor,
  unfinalized: true  // default
})

// Create unfinalized target with writer factory  
const dataTarget = target({
  writer: () => createWriter(),
  unfinalized: true  // default
})

// Execute pipeline
await pipe(dataSource, dataTarget)
```

### Finalized Pipeline

```typescript
// Create finalized source that handles forks internally
const dataSource = source({
  reader: (opts) => createReader(opts),
  cursor: blockCursor,
  unfinalized: false
})

// Create finalized target with optional fork handling
const dataTarget = target({
  writer: () => createWriter(),
  unfinalized: false
})

await pipe(dataSource, dataTarget)
```

<!-- Transformation Pipeline - Not yet implemented
```typescript
const transformerDuplex = transformer({
  transform: async (batch) => processedBatch,
  fork: async (fork, cursor) => transformedFork,
  cursor: outputCursor,
  unfinalized: true
})

source({
  reader: readerFactory,
  cursor: inputCursor
})
  .pipeThrough(transformerDuplex)
  .pipeTo(target({
    writer: writerFactory
  }))
```
-->

<!-- Multi-Output Fanout - Requires transformer implementation
```typescript
// Fan-out pattern for multiple consumers
const processedSource = source({
  reader,
  cursor: inputCursor
}).pipeThrough(transformer({
  transform: processor,
  fork: forkHandler,
  cursor: outputCursor
}))

await Promise.all([
  processedSource.pipeTo(target({ writer: primaryWriter })),
  processedSource.pipeTo(target({ writer: analyticsWriter })), 
  processedSource.pipeTo(target({ writer: auditWriter }))
])
```
-->

### Real-World Example

```typescript
// Example from solana-stream implementation
const dataSource = source({
    unfinalized: true,
    reader: async (opts) => {
        const stream = createDataStream(opts.offset)
        
        return {
            read: async () => {
                const batch = await stream.next()
                return batch.done ? undefined : batch.value
            },
            close: async () => stream.return?.()
        }
    },
    cursor: blockCursor
})

const dataTarget = target({
    unfinalized: true,
    writer: async () => {
        return {
            offset: undefined,
            write: async (batch) => {
                console.log(`${batch.offset.number}/${batch.head.number}`)
            },
            fork: async (fork) => {
                console.log(fork.heads[fork.heads.length - 1]?.number)
                return fork.heads[0]
            }
        }
    }
})

await dataSource.pipeTo(dataTarget)
```

## Fork Handling

The system uses two approaches for fork detection based on the unfinalized flag:

### Unfinalized Fork Handling (Exception-based)

1. Normal processing continues until a fork is detected
2. `ForkException` interrupts the flow with recovery information  
3. DataWriter's fork() method resolves fork and provides restart offset
4. Processing automatically resumes from the recovery point

```typescript
// Fork recovery flow for unfinalized targets
1. Source reads data → Target processes via DataWriter
2. Fork detected → ForkException thrown with fork information
3. DataWriter.fork() resolves fork → Returns recovery offset  
4. Processing restarts from recovery offset → Continues
```

### Finalized Fork Handling (Method-based)

1. Finalized DataWriters can optionally implement fork handling
2. Forks are handled through the optional `fork()` method
3. No exceptions are thrown during normal operation if forks are handled internally

## Implementation Details

### State Management

- **DataSource**: Manages reader lifecycle and offset tracking internally
- **DataTarget**: Tracks current head position and manages writer state
- **Transformations**: Use future-based coordination between target and source components

### Reference Flow

- Each operation receives its input ref from the previous operation
- Each operation specifies its output ref in the configuration
- References flow naturally from source to target through the pipeline

### Error Handling

- Unfinalized targets throw `ForkException` when forks occur
- Finalized targets handle forks through optional `fork()` method
- Automatic restart from resolved points in unfinalized mode
- Proper resource cleanup on errors

## Best Practices

1. **Choose Finalization Strategy**: Use `unfinalized: false` for targets that can handle forks internally, `unfinalized: true` (default) for simpler targets that rely on exception-based handling
2. **Use Overloaded Factory Functions**: TypeScript will enforce correct usage based on the `unfinalized` flag
3. **Manage References**: Ensure DataCursor implementations provide correct ordering and fork detection
4. **Resource Cleanup**: Always implement proper `close()` methods in readers and writers
5. **Fork Handling**: Implement appropriate fork handling for your use case - required for unfinalized writers, optional for finalized writers
6. **Error Handling**: Handle ForkExceptions appropriately when working with unfinalized sources and targets

## Performance Considerations

- Reader and writer factories are called as needed for lifecycle management
- State management uses 'opened', 'locked', 'closed' states to prevent concurrent access
- Offset management is optimized for restart scenarios after forks
- Resource cleanup prevents memory leaks in long-running pipelines
- The unfinalized/finalized system allows optimization based on fork handling needs
- AbortController is used for proper cancellation of long-running operations
