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
}

// Fork information when divergence is detected
export interface DataFork<T extends Data> {
    readonly heads: DataRef<T>[]           // All competing head references
}

// Reference comparison interface
export interface DataRefer<T extends Data> {
    get(ref: DataItem<T>): DataRef<T>                           // Get ref from item
    compare(a: DataRef<T>, b: DataRef<T>): 'ls' | 'eq' | 'gt' | 'fk'  // Compare refs
}
```

### Finalized vs Unfinalized System

The system distinguishes between finalized and unfinalized data processing:

- **Finalized**: Can handle forks internally without throwing exceptions
- **Unfinalized**: Throws `ForkException` when forks are detected, requiring external handling

## Core Interfaces

### Data Source Types

```typescript
// Base source interface
interface BaseDataSource<T extends Data> {
    ref: DataRefer<T>                                        // Reference system
    read(offset?: DataRef<T>): PromiseLike<DataBatch<T> | null>  // Read next batch
    close(): PromiseLike<void>                              // Clean up resources
}

// Unfinalized data source - can only pipe to unfinalized targets
export interface UnfinalizedDataSource<T extends Data> extends BaseDataSource<T> {
    finalized: false
    pipeThrough<U extends Data, F extends boolean>(duplex: {
        target: UnfinalizedDataTarget<T>
        source: DataSource<U, F>
    }): DataSource<U, F>
    pipeTo(target: UnfinalizedDataTarget<T>): PromiseLike<void>
}

// Finalized data source - can pipe to any target
export interface FinalizedDataSource<T extends Data> extends BaseDataSource<T> {
    finalized: true
    pipeThrough<U extends Data, F extends boolean>(duplex: {
        target: DataTarget<T>
        source: DataSource<U, F>
    }): DataSource<U, F>
    pipeTo(target: DataTarget<T>): PromiseLike<void>
}

// Union type with finalized flag extraction
export type DataSource<T extends Data, F extends boolean = any> = Extract<
    FinalizedDataSource<T> | UnfinalizedDataSource<T>,
    {finalized: F}
>
```

### Data Target Types

```typescript
// Base target interface
interface BaseDataTarget<T extends Data> {
    head(): PromiseLike<DataRef<T> | undefined>                       // Get current head
    write(batch: DataBatch<T>, ref: DataRefer<T>): PromiseLike<void>  // Process batch
    close(): PromiseLike<void>                                        // Clean up
}

// Finalized target - has optional fork handling
export interface FinalizedDataTarget<T extends Data> extends BaseDataTarget<T> {
    finalized: true
    fork?(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
}

// Unfinalized target - requires fork handling
export interface UnfinalizedDataTarget<T extends Data> extends BaseDataTarget<T> {
    finalized: false
    fork(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
}

// Union type
export type DataTarget<T extends Data> = FinalizedDataTarget<T> | UnfinalizedDataTarget<T>

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
    read(): PromiseLike<DataBatch<T> | null>
    close?(): PromiseLike<void>
}

// Base data writer interface
export interface DataWriter<T extends Data> {
    offset?: DataRef<T>
    write(batch: DataBatch<T>, ref: DataRefer<T>): PromiseLike<void>
    fork?(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
    close?(): PromiseLike<void>
}

// Unfinalized writer - requires fork handling
export interface UnfinalizedDataWriter<T extends Data> extends DataWriter<T> {
    fork(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
}
```

## Factory Functions

```typescript
// Create unfinalized data source
export function source<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized?: false
}): UnfinalizedDataSource<T>

// Create finalized data source  
export function source<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized: true
}): FinalizedDataSource<T>

// Create unfinalized data target
export function target<T extends Data>(config: {
    writer: () => PromiseLike<UnfinalizedDataWriter<T>>
    finalized?: false
}): UnfinalizedDataTarget<T>

// Create finalized data target
export function target<T extends Data>(config: {
    writer: () => PromiseLike<DataWriter<T>>
    finalized: true
}): FinalizedDataTarget<T>
```

## Reader Utilities

```typescript
// Factory utilities
export namespace DataReader {
    export function fromAsync<T extends Data>(
        iterator: AsyncIterableIterator<DataBatch<T>>
    ): DataReader<T>
}
```

## Pipeline Operations

```typescript
// Transform data batches - unfinalized version
export interface UnfinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork: (fork: DataFork<T>, ref: DataRefer<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    ref: DataRefer<U>
    finalized?: false
}

// Transform data batches - finalized version
export interface FinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork?: (fork: DataFork<T>, ref: DataRefer<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    ref: DataRefer<U>
    finalized: true
}

export function transformer<T extends Data, U extends Data>(
    config: UnfinalizedTransformerConfig<T, U>
): {target: UnfinalizedDataTarget<T>; source: UnfinalizedDataSource<U>}

export function transformer<T extends Data, U extends Data>(
    config: FinalizedTransformerConfig<T, U>
): {target: FinalizedDataTarget<T>; source: FinalizedDataSource<U>}

// Execute pipeline with automatic fork handling
export async function pipe<T extends Data>(
    source: DataSource<T>, 
    target: DataTarget<T>
): Promise<void>
```

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
  reader: (offset?) => createReader(offset),
  ref: blockRef,
  finalized: false  // default
})

// Create unfinalized target with writer factory  
const dataTarget = target({
  writer: () => createWriter(),
  finalized: false  // default
})

// Execute pipeline
await pipe(dataSource, dataTarget)
```

### Finalized Pipeline

```typescript
// Create finalized source that handles forks internally
const dataSource = source({
  reader: (offset?) => createReader(offset),
  ref: blockRef,
  finalized: true
})

// Create finalized target with optional fork handling
const dataTarget = target({
  writer: () => createWriter(),
  finalized: true
})

await pipe(dataSource, dataTarget)
```

### Transformation Pipeline

```typescript
const transformerDuplex = transformer({
  transform: async (batch) => processedBatch,
  fork: async (fork, ref) => transformedFork,
  ref: outputRef,
  finalized: false
})

source({
  reader: readerFactory,
  ref: inputRef
})
  .pipeThrough(transformerDuplex)
  .pipeTo(target({
    writer: writerFactory
  }))
```

### Multi-Output Fanout

```typescript
// Fan-out pattern for multiple consumers
const processedSource = source({
  reader,
  ref: inputRef
}).pipeThrough(transformer({
  transform: processor,
  fork: forkHandler,
  ref: outputRef
}))

await Promise.all([
  processedSource.pipeTo(target({ writer: primaryWriter })),
  processedSource.pipeTo(target({ writer: analyticsWriter })), 
  processedSource.pipeTo(target({ writer: auditWriter }))
])
```

### Using Reader Utilities

```typescript
// Create reader from async iterator
const reader = DataReader.fromAsync(asyncIterator)

const dataSource = source({
  reader: () => reader,
  ref: blockRef
})
```

## Fork Handling

The system uses two approaches for fork detection based on the finalized flag:

### Unfinalized Fork Handling (Exception-based)

1. Normal processing continues until a fork is detected
2. `ForkException` interrupts the flow with recovery information  
3. Target handles fork resolution and provides restart offset
4. Source automatically resumes from the recovery point

```typescript
// Fork recovery flow for unfinalized targets
1. Source reads data → Target processes
2. Fork detected → ForkException thrown with fork information
3. Target.fork() resolves fork → Returns recovery offset  
4. Source restarts from recovery offset → Processing continues
```

### Finalized Fork Handling (Method-based)

1. Finalized targets can optionally implement fork handling
2. Forks are handled through the `fork()` method call
3. No exceptions are thrown during normal operation

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

1. **Choose Finalization Strategy**: Use `finalized: true` for targets that can handle forks internally, `finalized: false` for simpler targets that rely on exception-based handling
2. **Use Overloaded Factory Functions**: TypeScript will enforce correct usage based on the `finalized` flag
3. **Manage References**: Specify output refs in transformer configurations
4. **Resource Cleanup**: Always implement proper `close()` methods
5. **State Isolation**: Keep transformation state isolated and cleanable
6. **Fork Handling**: Implement appropriate fork handling for your use case

## Performance Considerations

- Reader and writer factories are called as needed for lifecycle management
- Internal buffering in transformations uses future-based coordination
- Offset management is optimized for restart scenarios
- Resource cleanup prevents memory leaks in long-running pipelines
- The finalized/unfinalized system allows optimization based on fork handling needs
