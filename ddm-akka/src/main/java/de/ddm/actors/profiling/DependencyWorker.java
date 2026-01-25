package de.ddm.actors.profiling;

import java.util.HashMap;
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

	public static class ColumnCacheMessage implements Message, LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = 1L;
		
		private int fileId;
		private int columnIndex;
		private Set<String> values;
		private int totalMessages;

		public ColumnCacheMessage() {}

		public ColumnCacheMessage(int fileId, int columnIndex, Set<String> values, int totalMessages) {
			this.fileId = fileId;
			this.columnIndex = columnIndex;
			this.values = values;
			this.totalMessages = totalMessages;
		}

		public int getFileId() { return fileId; }
		public int getColumnIndex() { return columnIndex; }
		public Set<String> getValues() { return values; }
		public int getTotalMessages() { return totalMessages; }
		
		public void setFileId(int fileId) { this.fileId = fileId; }
		public void setColumnIndex(int columnIndex) { this.columnIndex = columnIndex; }
		public void setValues(Set<String> values) { this.values = values; }
		public void setTotalMessages(int totalMessages) { this.totalMessages = totalMessages; }
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


	private Map<String, Set<String>> workerColumnCache = new HashMap<>();
	private boolean cacheInitialized = false;
	private int expectedCacheMessages = 0;
	private int receivedCacheMessages = 0;

	

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(ColumnCacheMessage.class, this::handleColumnCache)
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

	private Behavior<Message> handleColumnCache(ColumnCacheMessage message) {
	// Initialize expected count on first message
		if (expectedCacheMessages == 0) {
			expectedCacheMessages = message.getTotalMessages();
			getContext().getLog().info("Worker expecting {} cache messages", expectedCacheMessages);
		}
		
		// Store column data in cache
		String key = message.getFileId() + ":" + message.getColumnIndex();
		workerColumnCache.put(key, message.getValues());
		receivedCacheMessages++;
		
		getContext().getLog().info("Cached column {}/{} (file {} col {}, {} values)",
			receivedCacheMessages, expectedCacheMessages,
			message.getFileId(), message.getColumnIndex(), message.getValues().size());
		
		// Check if cache is fully loaded
		if (receivedCacheMessages == expectedCacheMessages) {
			cacheInitialized = true;
			getContext().getLog().info("Worker cache fully initialized! Ready for tasks.");

			// ✅ Request work ONCE, after cache is ready
			if (minerRef != null) {
				minerRef.tell(new DependencyMiner.WorkRequest(
					getContext().getSelf(),
					this.largeMessageProxy
				));
			} else {
				getContext().getLog().error("Miner reference not set!");
			}
		}


		
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		// Check if cache is ready
		if (!cacheInitialized) {
			getContext().getLog().warn("Task received but cache not ready yet!");
			// Request work again after cache is ready
			// message.getMiner().tell(new DependencyMiner.WorkRequest(
			// 	getContext().getSelf(),
			// 	this.largeMessageProxy
			// ));
			return this;
		}

		boolean violated = false;

		try {
			// ✅ READ FROM CACHE instead of message!
			String depKey = message.getDependentFileId() + ":" + message.getDependentColumnIndex();
			String refKey = message.getReferencedFileId() + ":" + message.getReferencedColumnIndex();
			
			Set<String> dependentValues = workerColumnCache.get(depKey);
			Set<String> referencedValues = workerColumnCache.get(refKey);

			if (dependentValues == null || referencedValues == null) {
				getContext().getLog().error("Cache miss! Keys: {} {}", depKey, refKey);
				violated = true;
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
}