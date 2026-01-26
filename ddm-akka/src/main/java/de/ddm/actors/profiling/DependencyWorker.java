package de.ddm.actors.profiling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.ColumnIdentifier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	public static class ReceptionistListingMessage implements Message {
		private final Receptionist.Listing listing;

		public ReceptionistListingMessage(Receptionist.Listing listing) {
			this.listing = listing;
		}

		public Receptionist.Listing getListing() {
			return listing;
		}
	}

	public static class TaskMessage implements Message, LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -4667745204456518160L;

		private int dependentFileId;
		private int dependentColumnIndex;
		private int referencedFileId;
		private int referencedColumnIndex;

		private ActorRef<DependencyMiner.Message> miner;
		private ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;

		private java.util.Set<String> dependentValues;
		private java.util.Set<String> referencedValues;

		public TaskMessage(
				int dependentFileId,
				int dependentColumnIndex,
				int referencedFileId,
				int referencedColumnIndex,
				ActorRef<DependencyMiner.Message> miner,
				ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy,
				java.util.Set<String> dependentValues,
				java.util.Set<String> referencedValues
		) {
			this.dependentFileId = dependentFileId;
			this.dependentColumnIndex = dependentColumnIndex;
			this.referencedFileId = referencedFileId;
			this.referencedColumnIndex = referencedColumnIndex;
			this.miner = miner;
			this.dependencyMinerLargeMessageProxy = dependencyMinerLargeMessageProxy;
			this.dependentValues = dependentValues;
			this.referencedValues = referencedValues;
		}

		public TaskMessage() {}

		public int getDependentFileId() {
			return dependentFileId;
		}

		public int getDependentColumnIndex() {
			return dependentColumnIndex;
		}

		public int getReferencedFileId() {
			return referencedFileId;
		}

		public int getReferencedColumnIndex() {
			return referencedColumnIndex;
		}

		public ActorRef<DependencyMiner.Message> getMiner() {
			return miner;
		}

		public ActorRef<LargeMessageProxy.Message> getDependencyMinerLargeMessageProxy() {
			return dependencyMinerLargeMessageProxy;
		}

		public java.util.Set<String> getDependentValues() {
			return dependentValues;
		}

		public java.util.Set<String> getReferencedValues() {
			return referencedValues;
		}

		// Setters for Jackson deserialization
		public void setDependentFileId(int value) {
			this.dependentFileId = value;
		}

		public void setDependentColumnIndex(int value) {
			this.dependentColumnIndex = value;
		}

		public void setReferencedFileId(int value) {
			this.referencedFileId = value;
		}

		public void setReferencedColumnIndex(int value) {
			this.referencedColumnIndex = value;
		}

		public void setMiner(ActorRef<DependencyMiner.Message> value) {
			this.miner = value;
		}

		public void setDependencyMinerLargeMessageProxy(ActorRef<LargeMessageProxy.Message> value) {
			this.dependencyMinerLargeMessageProxy = value;
		}

		public void setDependentValues(java.util.Set<String> value) {
			this.dependentValues = value;
		}

		public void setReferencedValues(java.util.Set<String> value) {
			this.referencedValues = value;
		}
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnAssignmentMessage implements Message {
		private static final long serialVersionUID = 1L;
		private List<de.ddm.structures.ColumnIdentifier> assignedColumns;
		private int totalWorkers;
		private int expectedFileCount;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnDataRequest implements Message, LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = 1L;
		private de.ddm.structures.ColumnIdentifier columnId;
		private ActorRef<DependencyWorker.Message> requester;
		private ActorRef<LargeMessageProxy.Message> requesterProxy;
		private String requestId; // To track which IND check this is for
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor  
	public static class ColumnDataResponse implements Message, LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = 1L;
		private de.ddm.structures.ColumnIdentifier columnId;
		private Set<String> values;
		private String requestId; // Match with request
	}

	

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private ActorRef<DependencyMiner.Message> minerRef;


	// Column ownership (NEW approach)
	private List<de.ddm.structures.ColumnIdentifier> assignedColumns = new ArrayList<>();
	private Map<de.ddm.structures.ColumnIdentifier, Set<String>> ownedColumnData = new HashMap<>();

	// Track data loading progress
	private int expectedFileCount = 0;
	private Set<Integer> completedFiles = new HashSet<>();
	private boolean dataLoadingComplete = false;

	

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(ColumnAssignmentMessage.class, this::handle)
				.onMessage(DependencyMiner.BatchMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners) {

			this.minerRef = dependencyMiner; 

			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(
				this.getContext().getSelf(),
				this.largeMessageProxy  // ✅ Send our proxy!
			));
			// dependencyMiner.tell(new DependencyMiner.WorkRequest(
			// 	this.getContext().getSelf(),
			// 	this.largeMessageProxy  // Send our proxy reference!
			// ));
}
		return this;
	}

	

	private Behavior<Message> handle(TaskMessage message) {  
		// Check if data is ready
		if (!dataLoadingComplete) {
			getContext().getLog().warn("Task received but data not loaded yet!");
			return this;
		}

		boolean violated = false;

		try {
			ColumnIdentifier depCol = new ColumnIdentifier(
				message.getDependentFileId(), 
				message.getDependentColumnIndex()
			);
			ColumnIdentifier refCol = new ColumnIdentifier(
				message.getReferencedFileId(), 
				message.getReferencedColumnIndex()
			);
			
			Set<String> dependentValues = ownedColumnData.get(depCol);
			Set<String> referencedValues = ownedColumnData.get(refCol);

			// IMPORTANT: Missing data ≠ violation
			if (dependentValues == null || referencedValues == null) {
				// Cannot disprove IND → treat as NOT violated
				violated = false;
			} else {
				// Check subset relationship
				for (String value : dependentValues) {
					if (!referencedValues.contains(value)) {
						violated = true;
						break;
					}
				}
			}


		} catch (Exception e) {
			violated = true;
			getContext().getLog().error("Error checking IND: {}", e.getMessage());
		}

		DependencyMiner.UnaryIndResult result =
			new DependencyMiner.UnaryIndResult(
				message.getDependentFileId(),
				message.getDependentColumnIndex(),
				message.getReferencedFileId(),
				message.getReferencedColumnIndex(),
				violated
			);

		// Send result back via LargeMessageProxy
		this.largeMessageProxy.tell(
			new LargeMessageProxy.SendMessage(
				result,
				message.getDependencyMinerLargeMessageProxy()
			)
		);

		// Request more work
		message.getMiner().tell(
			new DependencyMiner.WorkRequest(
				getContext().getSelf(),
				this.largeMessageProxy
			)
		);

		return this;
	}

	private Behavior<Message> handle(ColumnAssignmentMessage message) {
		this.assignedColumns = message.getAssignedColumns();
		this.expectedFileCount = message.getExpectedFileCount();
		
		getContext().getLog().info("✅ ColumnAssignmentMessage received: assignedColumns={}, expectedFileCount={}", 
			assignedColumns.size(), expectedFileCount);
		
		// Initialize data structures for owned columns
		for (ColumnIdentifier colId : assignedColumns) {
			ownedColumnData.put(colId, new HashSet<>());
		}
		
		// LOG WHICH FILES WE HAVE COLUMNS FROM
		Set<Integer> filesWithColumns = new HashSet<>();
		for (ColumnIdentifier colId : assignedColumns) {
			filesWithColumns.add(colId.getFileId());
		}
		
		getContext().getLog().info("Worker assigned {} columns from {} files. Files with assigned columns: {}", 
			assignedColumns.size(), expectedFileCount, filesWithColumns);
		
		// ✅ CRITICAL: Check if files already completed before assignment arrived
		getContext().getLog().info("Current completedFiles at assignment: {} (size={})", 
			completedFiles, completedFiles.size());
		
		if (completedFiles.size() > 0) {
			getContext().getLog().info("⚠️  Found {} files already completed before assignment: {}", 
				completedFiles.size(), completedFiles);
			
			// Check if all files are done
			if (completedFiles.size() == expectedFileCount) {
				dataLoadingComplete = true;
				getContext().getLog().info("🎉 All data already loaded! Requesting work...");
				
				if (minerRef != null) {
					minerRef.tell(new DependencyMiner.WorkRequest(
						getContext().getSelf(),
						this.largeMessageProxy
					));
				} else {
					getContext().getLog().error("❌ Cannot request work - minerRef is null!");
				}
			} else {
				getContext().getLog().info("Partial completion: {}/{} files done", 
					completedFiles.size(), expectedFileCount);
			}
		} else {
			getContext().getLog().info("No files completed yet at assignment time - will track as they arrive");
		}
		
		return this;
	}

	private Behavior<Message> handle(DependencyMiner.BatchMessage message) {
		int fileId = message.getId();
		List<String[]> batch = message.getBatch();
		
		getContext().getLog().debug("Received batch from file {}: {} rows, isEmpty={}", 
			fileId, batch.size(), batch.isEmpty());
		
		// Track empty batches (file completion)
		if (batch.isEmpty()) {
			// Check if already marked complete (avoid duplicates)
			if (completedFiles.contains(fileId)) {
				getContext().getLog().warn("File {} sent completion signal again (duplicate)", fileId);
				return this;
			}
			
			completedFiles.add(fileId);
			
			// Log completion based on whether we know expectedFileCount
			if (expectedFileCount > 0) {
				getContext().getLog().info("✅ File {} complete, {} of {} files done. Completed: {}", 
					fileId, completedFiles.size(), expectedFileCount, completedFiles);
				
				// Check if all files done
				if (completedFiles.size() == expectedFileCount) {
					dataLoadingComplete = true;
					getContext().getLog().info("🎉 All data loaded ({}/{} files)! Requesting work...", 
						completedFiles.size(), expectedFileCount);
					
					if (minerRef != null) {
						minerRef.tell(new DependencyMiner.WorkRequest(
							getContext().getSelf(),
							this.largeMessageProxy
						));
					} else {
						getContext().getLog().error("❌ Cannot request work - minerRef is null!");
					}
				}
			} else {
				// expectedFileCount not set yet - file completed before assignment
				getContext().getLog().info("⚠️  File {} marked complete (total: {}), but expectedFileCount not set yet (waiting for ColumnAssignmentMessage)", 
					fileId, completedFiles.size());
			}
			
			return this;
		}
		
		// For data batches, need column assignments to process
		if (assignedColumns.isEmpty()) {
			getContext().getLog().debug("Ignoring data batch from file {} - no columns assigned yet", fileId);
			return this;
		}
		
		// Process batch - extract our columns only
		int processedRows = 0;
		for (String[] row : batch) {
			for (ColumnIdentifier colId : assignedColumns) {
				if (colId.getFileId() == fileId && colId.getColumnIndex() < row.length) {
					ownedColumnData.get(colId).add(row[colId.getColumnIndex()]);
					processedRows++;
				}
			}
		}
		
		getContext().getLog().debug("Processed {} data rows from file {} for owned columns", 
			processedRows, fileId);
		
		return this;
	}
}