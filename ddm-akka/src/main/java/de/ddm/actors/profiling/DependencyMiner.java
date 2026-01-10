	package de.ddm.actors.profiling;

	import java.io.File;
	import java.util.ArrayList;
	import java.util.List;

	import akka.actor.typed.ActorRef;
	import akka.actor.typed.Behavior;
	import akka.actor.typed.Terminated;
	import akka.actor.typed.javadsl.AbstractBehavior;
	import akka.actor.typed.javadsl.ActorContext;
	import akka.actor.typed.javadsl.Behaviors;
	import akka.actor.typed.javadsl.Receive;
	import akka.actor.typed.receptionist.Receptionist;
	import akka.actor.typed.receptionist.ServiceKey;
	import de.ddm.actors.patterns.LargeMessageProxy;
	import de.ddm.serialization.AkkaSerializable;
	import de.ddm.singletons.InputConfigurationSingleton;
	import de.ddm.singletons.SystemConfigurationSingleton;
	import de.ddm.structures.InclusionDependency;
	import lombok.AllArgsConstructor;
	import lombok.Getter;
	import lombok.NoArgsConstructor;
	import lombok.Setter;

		public class DependencyMiner extends AbstractBehavior<de.ddm.actors.profiling.DependencyMiner.Message> {

			////////////////////
			// Actor Messages //
			////////////////////

			public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
			}

			@NoArgsConstructor
			public static class StartMessage implements DependencyMiner.Message {
				private static final long serialVersionUID = -1963913294517850454L;
			}


			@NoArgsConstructor
			@AllArgsConstructor
			@Getter
			public static class HeaderMessage implements DependencyMiner.Message {
				private static final long serialVersionUID = -5322425954432915838L;

				int id;
				String[] header;
			}


			@NoArgsConstructor
			@AllArgsConstructor
			@Getter
			public static class BatchMessage implements DependencyMiner.Message {
				private static final long serialVersionUID = 4591192372652568030L;

				int id;
				List<String[]> batch;
			}


			@NoArgsConstructor
			@AllArgsConstructor
			@Getter
			public static class RegistrationMessage implements Message {
				private ActorRef<DependencyWorker.Message> dependencyWorker;
			}






			@NoArgsConstructor
			@AllArgsConstructor
			@Getter
			public static class RequestWorkMessage implements Message {
				private static final long serialVersionUID = 1L;
				private ActorRef<DependencyWorker.Message> worker;
				private ActorRef<LargeMessageProxy.Message> workerProxy;
			}




	//	@Getter
	//	@NoArgsConstructor
	//	@AllArgsConstructor
	//	public static class CompletionMessage implements Message {
	//		private static final long serialVersionUID = -7642425159675583598L;
	//		ActorRef<DependencyWorker.Message> dependencyWorker;
	//		int result;
	//	}

			@Getter
			@Setter
			@NoArgsConstructor
	//		@AllArgsConstructor
			public static class UnaryIndResult implements Message {
				private static final long serialVersionUID = 1L;

				private int dependentFileId;
				private int dependentColumnIndex;
				private int referencedFileId;
				private int referencedColumnIndex;
				private boolean violated;

				public UnaryIndResult(
						int dependentFileId,
						int dependentColumnIndex,
						int referencedFileId,
						int referencedColumnIndex,
						boolean violated
				) {
					this.dependentFileId = dependentFileId;
					this.dependentColumnIndex = dependentColumnIndex;
					this.referencedFileId = referencedFileId;
					this.referencedColumnIndex = referencedColumnIndex;
					this.violated = violated;
				}
			}

			@NoArgsConstructor
			@AllArgsConstructor
			@Getter
			@Setter
			public static class ColumnDataMessage implements Message {
				private static final long serialVersionUID = 1L;
				private int fileId;
				private int columnIndex;
				private java.util.Set<String> values;
			}

			




			////////////////////////
			// Actor Construction //
			////////////////////////

			public static final String DEFAULT_NAME = "dependencyMiner";

			public static final ServiceKey<de.ddm.actors.profiling.DependencyMiner.Message> dependencyMinerService = ServiceKey.create(de.ddm.actors.profiling.DependencyMiner.Message.class, DEFAULT_NAME + "Service");

			public static Behavior<de.ddm.actors.profiling.DependencyMiner.Message> create() {
				return Behaviors.setup(de.ddm.actors.profiling.DependencyMiner::new);
			}

			private DependencyMiner(ActorContext<de.ddm.actors.profiling.DependencyMiner.Message> context) {
				super(context);
				this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
				this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
				this.headerLines = new String[this.inputFiles.length][];

				this.inputReaders = new ArrayList<>(inputFiles.length);
				for (int id = 0; id < this.inputFiles.length; id++)
					this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
				this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
				this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), LargeMessageProxy.DEFAULT_NAME);

				this.dependencyWorkers = new ArrayList<>();

				context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
			}

			/////////////////
			// Actor State //
			/////////////////

			private long startTime;

			private final boolean discoverNaryDependencies;
			private final File[] inputFiles;
			private final String[][] headerLines;

			private final List<ActorRef<InputReader.Message>> inputReaders;
			private final ActorRef<ResultCollector.Message> resultCollector;
			private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

			private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
	//		private boolean workersRegistered = false;

			private int nextDependentFile = 0;
			private int nextDependentColumn = 0;
			private int nextReferencedFile = 0;
			private int nextReferencedColumn = 0;

			// Total number of unary IND checks
			private int totalTasks;

			// How many tasks have been sent
			private int tasksIssued = 0;

			// How many results have been received
			private int resultsReceived = 0;

			// Flag to stop sending new work
			private boolean noMoreTasks = false;

			private boolean started = false;

			private int inFlightTasks = 0;

			// Storage for column data received from InputReaders
			private final java.util.Map<String, java.util.Set<String>> columnData = new java.util.HashMap<>();

			// Track when all column data is loaded
			private int expectedColumns = 0;
			private int receivedColumns = 0;
			private boolean allColumnsLoaded = false;


			////////////////////
			// Actor Behavior //
			////////////////////

			@Override
			public Receive<de.ddm.actors.profiling.DependencyMiner.Message> createReceive() {
				return newReceiveBuilder()
						.onMessage(de.ddm.actors.profiling.DependencyMiner.StartMessage.class, this::handle)
						.onMessage(de.ddm.actors.profiling.DependencyMiner.BatchMessage.class, this::handle)
						.onMessage(de.ddm.actors.profiling.DependencyMiner.HeaderMessage.class, this::handle)
						.onMessage(de.ddm.actors.profiling.DependencyMiner.RegistrationMessage.class, this::handle)
						.onMessage(de.ddm.actors.profiling.DependencyMiner.RequestWorkMessage.class, this::handle)
	//				.onMessage(CompletionMessage.class, this::handle)
						.onMessage(de.ddm.actors.profiling.DependencyMiner.UnaryIndResult.class, this::handle)
						.onMessage(DependencyMiner.ColumnDataMessage.class, this::handle)


						.onSignal(Terminated.class, this::handle)
						.build();
			}

			private void advanceIndices() {

				nextReferencedColumn++;

				if (nextReferencedColumn >= headerLines[nextReferencedFile].length) {
					nextReferencedColumn = 0;
					nextReferencedFile++;
				}

				if (nextReferencedFile >= inputFiles.length) {
					nextReferencedFile = 0;
					nextDependentColumn++;
				}

				if (nextDependentColumn >= headerLines[nextDependentFile].length) {
					nextDependentColumn = 0;
					nextDependentFile++;
				}
			}

		private Behavior<Message> handle(StartMessage message) {
			for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
				inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));

			// ALWAYS start reading data on StartMessage
			for (ActorRef<InputReader.Message> inputReader : this.inputReaders) {
				inputReader.tell(
						new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000)
				);
			}

			getContext().getLog().info("Started reading input data");


			this.started = true;
			this.startTime = System.currentTimeMillis();
			return this;
		}

		private Behavior<Message> handle(HeaderMessage message) {
			this.headerLines[message.getId()] = message.getHeader();

			boolean allHeadersLoaded = true;
			for (String[] h : headerLines) {
				if (h == null) {
					allHeadersLoaded = false;
					break;
				}
			}

			if (allHeadersLoaded && totalTasks == 0) {
				for (int df = 0; df < inputFiles.length; df++) {
					for (int dc = 0; dc < headerLines[df].length; dc++) {
						for (int rf = 0; rf < inputFiles.length; rf++) {
							for (int rc = 0; rc < headerLines[rf].length; rc++) {
								if (!(df == rf && dc == rc)) {
									totalTasks++;
								}
							}
						}
					}
				}

				getContext().getLog().info("Total unary IND tasks: {}", totalTasks);
            
				// Calculate expected columns
				for (int f = 0; f < inputFiles.length; f++) {
					expectedColumns += headerLines[f].length;
				}
				getContext().getLog().info("Expecting {} columns total", expectedColumns);
			}
	//      for (ActorRef<DependencyWorker.Message> worker : dependencyWorkers) {
	//              worker.tell(new DependencyMiner.RequestWorkMessage(worker));
	//      }
			return this;
	}

		private Behavior<Message> handle(BatchMessage message) {
			// Ignoring batch content for now ... but I could do so much with it.

	//		System.out.println(MemoryUtils.byteSizeOf(message.getBatch()));
	//		System.out.println(MemoryUtils.bytesMax() + "    " + MemoryUtils.bytesFree());

			if (!message.getBatch().isEmpty())
				this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
			return this;
		}

		private Behavior<Message> handle(RegistrationMessage message) {
			ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
			if (!this.dependencyWorkers.contains(dependencyWorker)) {
				this.dependencyWorkers.add(dependencyWorker);
				this.getContext().watch(dependencyWorker);
				// The worker should get some work ... let me send her something before I figure out what I actually want from her.
				// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)

	//			dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));
	//			if (!workersRegistered) {
	//				workersRegistered = true;
	//			}

				// If StartMessage already happened, start reading now
	//			if (started) {
	//				for (ActorRef<InputReader.Message> inputReader : inputReaders) {
	//					inputReader.tell(
	//							new InputReader.ReadBatchMessage(getContext().getSelf(), 10000)
	//					);
	//				}
	//			}


			}
			return this;
		}

	//	private Behavior<Message> handle(CompletionMessage message) {
	//		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
	//		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.
	//
	//		if (this.headerLines[0] != null) {
	//			Random random = new Random();
	//			int dependent = random.nextInt(this.inputFiles.length);
	//			int referenced = random.nextInt(this.inputFiles.length);
	//			File dependentFile = this.inputFiles[dependent];
	//			File referencedFile = this.inputFiles[referenced];
	//			String[] dependentAttributes = {this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)], this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)]};
	//			String[] referencedAttributes = {this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)], this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)]};
	//			InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
	//			List<InclusionDependency> inds = new ArrayList<>(1);
	//			inds.add(ind);
	//
	//			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
	//		}
	//		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
	//		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!
	//
	////		dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));
	//
	//		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
	//		if (System.currentTimeMillis() - this.startTime > 120000)
	//			this.end();
	//		return this;
	//	}

		private void end() {
			this.resultCollector.tell(new ResultCollector.FinalizeMessage());
			long discoveryTime = System.currentTimeMillis() - this.startTime;
			this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
		}

		private Behavior<Message> handle(Terminated signal) {
			ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
			this.dependencyWorkers.remove(dependencyWorker);
			return this;
		}

		private Behavior<Message> handle(RequestWorkMessage message) {
			if (headerLines[0] == null || noMoreTasks) return this;

			while (nextDependentFile == nextReferencedFile &&
					nextDependentColumn == nextReferencedColumn) {
				advanceIndices();
			}

			if (nextDependentFile >= inputFiles.length) {
				noMoreTasks = true;
				return this;
			}

			// Get column data from storage
			String depKey = nextDependentFile + ":" + nextDependentColumn;
			String refKey = nextReferencedFile + ":" + nextReferencedColumn;
			
			java.util.Set<String> depValues = columnData.get(depKey);
			java.util.Set<String> refValues = columnData.get(refKey);
			
			// Only send task if we have the data
			if (depValues != null && refValues != null) {
				DependencyWorker.TaskMessage task = new DependencyWorker.TaskMessage(
					nextDependentFile,
					nextDependentColumn,
					nextReferencedFile,
					nextReferencedColumn,
					getContext().getSelf(),
					this.largeMessageProxy,
					depValues,
					refValues
				);
				
				this.largeMessageProxy.tell(
					new LargeMessageProxy.SendMessage(
						task,
						message.getWorkerProxy()  
					)
				);
				
				tasksIssued++;
				inFlightTasks++;
			}
			
			advanceIndices();
			return this;
		}


			private Behavior<Message> handle(UnaryIndResult message) {

				resultsReceived++;
				inFlightTasks--;


				if (!message.isViolated()) {
				InclusionDependency ind = new InclusionDependency(
						inputFiles[message.getDependentFileId()],
						new String[]{headerLines[message.getDependentFileId()][message.getDependentColumnIndex()]},
						inputFiles[message.getReferencedFileId()],
						new String[]{headerLines[message.getReferencedFileId()][message.getReferencedColumnIndex()]}
				);

				this.resultCollector.tell(
						new ResultCollector.ResultMessage(List.of(ind))
				);
			}
			if (noMoreTasks && inFlightTasks == 0) {
				getContext().getLog().info(
						"All unary INDs processed ({} tasks). Terminating.",
						totalTasks
				);
				end();
			}

			return this;
		}

		private Behavior<Message> handle(ColumnDataMessage message) {
			String key = message.getFileId() + ":" + message.getColumnIndex();
			columnData.put(key, message.getValues());
			receivedColumns++;
			
			getContext().getLog().info("Stored column data for file {} col {} ({} values) - {}/{} columns loaded",
				message.getFileId(), message.getColumnIndex(), message.getValues().size(),
				receivedColumns, expectedColumns);
			
			// When all columns loaded, kick workers to request work!
			if (!allColumnsLoaded && receivedColumns == expectedColumns) {
				allColumnsLoaded = true;
				getContext().getLog().info("All column data loaded! Kicking {} workers to start processing", 
					dependencyWorkers.size());
				
				// Note: We can't kick workers here anymore since we don't have their proxy refs!
// Workers will request work after registering, and the check in handle(RequestWorkMessage)
// will handle cases where data isn't ready yet by just returning without sending a task.
// Workers will retry by sending RequestWorkMessage again when other workers finish tasks.
			}
			
			return this;
		}

	}