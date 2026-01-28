package de.ddm.actors.profiling;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import de.ddm.structures.ColumnIdentifier;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

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
	public static class BatchMessage implements DependencyMiner.Message, DependencyWorker.Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	public static class RegistrationMessage implements Message {
		private ActorRef<DependencyWorker.Message> dependencyWorker;
		private ActorRef<LargeMessageProxy.Message> workerProxy;
	}

	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	public static class WorkRequest implements Message {
		private static final long serialVersionUID = 1L;
		private ActorRef<DependencyWorker.Message> worker;
		private ActorRef<LargeMessageProxy.Message> workerProxy;
	}

	@Getter
	@Setter
	@NoArgsConstructor
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

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = 
		ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<DependencyMiner.Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<DependencyMiner.Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), 
				InputReader.DEFAULT_NAME + "_" + id));
		
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(
			LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), 
			LargeMessageProxy.DEFAULT_NAME);

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
	private final List<ActorRef<LargeMessageProxy.Message>> workerProxies = new ArrayList<>();

	private int nextDependentFile = 0;
	private int nextDependentColumn = 0;
	private int nextReferencedFile = 0;
	private int nextReferencedColumn = 0;

	private int totalTasks;
	private int tasksIssued = 0;
	private int resultsReceived = 0;
	private boolean noMoreTasks = false;
	private boolean started = false;
	private int inFlightTasks = 0;

	// Column ownership tracking for partitioning
	private final Map<ColumnIdentifier, ActorRef<DependencyWorker.Message>> columnOwnership = new HashMap<>();
	private int workerCounter = 0;
	private int filesReadComplete = 0;
	private final Set<Integer> completedFiles = new HashSet<>();

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<DependencyMiner.Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(WorkRequest.class, this::handle)
				.onMessage(UnaryIndResult.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private void moveToNextTaskPair() {
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

		for (ActorRef<InputReader.Message> inputReader : this.inputReaders) {
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
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
			getContext().getLog().info("All headers loaded, {} files total", inputFiles.length);
		}

		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		if (message.getBatch().isEmpty()) {
			completedFiles.add(message.getId());
			getContext().getLog().info("File {} finished reading (EOF recorded)", message.getId());
		}
		
		// Forward batch via proxy to handle messages larger than 256KB
		for (int i = 0; i < dependencyWorkers.size(); i++) {
			ActorRef<DependencyWorker.Message> worker = dependencyWorkers.get(i);
			ActorRef<LargeMessageProxy.Message> workerProxy = workerProxies.get(i);
			
			this.largeMessageProxy.tell(
				new LargeMessageProxy.SendMessage(message, workerProxy)
			);
		}
		
		if (!message.getBatch().isEmpty()) {
			this.inputReaders.get(message.getId()).tell(
				new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000)
			);
		}
		
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		ActorRef<LargeMessageProxy.Message> workerProxy = message.getWorkerProxy();
		
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.workerProxies.add(workerProxy);
			this.getContext().watch(dependencyWorker);
			
			// Assign columns using round-robin distribution
			List<ColumnIdentifier> assignedColumns = assignColumnsToWorker();

			dependencyWorker.tell(new DependencyWorker.ColumnAssignmentMessage(
				assignedColumns, 
				this.dependencyWorkers.size(),
				this.inputFiles.length
			));

			getContext().getLog().info("Assigned {} columns to worker {}", 
				assignedColumns.size(), this.dependencyWorkers.size());

			// Replay EOF signals for files that finished before this worker joined
			for (int fileId : completedFiles) {
				dependencyWorker.tell(new BatchMessage(fileId, List.of()));
			}

			if (!completedFiles.isEmpty()) {
				getContext().getLog().info("Replayed {} EOF signals to new worker", 
					completedFiles.size());
			}
		}
		return this;
	}

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

	private Behavior<Message> handle(WorkRequest message) {
		if (headerLines[0] == null || noMoreTasks) return this;

		while (nextDependentFile < inputFiles.length) {
			String depColName = headerLines[nextDependentFile][nextDependentColumn];
			String refColName = headerLines[nextReferencedFile][nextReferencedColumn];

			if (nextDependentFile == nextReferencedFile && nextDependentColumn == nextReferencedColumn) {
				moveToNextTaskPair();
				continue;
			}

			// Semantic pruning: only check KEY columns
			if (!depColName.endsWith("KEY") || !refColName.endsWith("KEY")) {
				moveToNextTaskPair();
				continue;
			}

			break;
		}

		if (nextDependentFile >= inputFiles.length) {
			noMoreTasks = true;
			return this;
		}

		DependencyWorker.TaskMessage task = new DependencyWorker.TaskMessage(
			nextDependentFile,
			nextDependentColumn,
			nextReferencedFile,
			nextReferencedColumn,
			getContext().getSelf(),
			this.largeMessageProxy,
			null,
			null
		);

		message.getWorker().tell(task);

		tasksIssued++;
		inFlightTasks++;
		moveToNextTaskPair();
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

			this.resultCollector.tell(new ResultCollector.ResultMessage(List.of(ind)));
		}
		
		if (noMoreTasks && inFlightTasks == 0) {
			getContext().getLog().info("All unary INDs processed ({} tasks). Terminating.", totalTasks);
			end();
		}

		return this;
	}

	private List<ColumnIdentifier> assignColumnsToWorker() {
		List<ColumnIdentifier> assigned = new ArrayList<>();
		
		if (headerLines[0] == null) {
			return assigned;
		}
		
		int workerIndex = workerCounter++;
		int numWorkers = Math.max(1, this.dependencyWorkers.size());
		
		// Round-robin column distribution across workers
		for (int fileId = 0; fileId < inputFiles.length; fileId++) {
			for (int colIdx = workerIndex; colIdx < headerLines[fileId].length; colIdx += numWorkers) {
				ColumnIdentifier colId = new ColumnIdentifier(fileId, colIdx);
				assigned.add(colId);
				columnOwnership.put(colId, dependencyWorkers.get(dependencyWorkers.size() - 1));
			}
		}
		
		return assigned;
	}

	private ActorRef<DependencyWorker.Message> getColumnOwner(ColumnIdentifier colId) {
		return columnOwnership.get(colId);
	}

	private boolean isPrimaryKey(int fileId, String columnName) {
		String[] headers = headerLines[fileId];

		int keyCount = 0;
		for (String h : headers) {
			if (h.endsWith("KEY")) {
				keyCount++;
			}
		}

		if (keyCount > 1) {
			return false;
		}

		return columnName.endsWith("KEY");
	}
}