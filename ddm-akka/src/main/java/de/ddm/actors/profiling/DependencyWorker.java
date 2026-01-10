package de.ddm.actors.profiling;

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

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners) {
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
			dependencyMiner.tell(new DependencyMiner.WorkRequest(
				this.getContext().getSelf(),
				this.largeMessageProxy  // Send our proxy reference!
			));
}
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {

		if (message.getDependentValues() == null || message.getReferencedValues() == null) {
			message.getMiner().tell(new DependencyMiner.WorkRequest(
				getContext().getSelf(),
				this.largeMessageProxy  // ✅ Add proxy reference!
			));
			return this;
		}

		boolean violated = false;


		try {
			// USE DATA FROM MESSAGE - NO FILE ACCESS!
			Set<String> dependentValues = message.getDependentValues();	
			Set<String> referencedValues = message.getReferencedValues();

			// Check subset relationship
			for (String value : dependentValues) {
				if (!referencedValues.contains(value)) {
					violated = true;
					break;
				}
			}

		} catch (Exception e) {
			violated = true; // fail-safe
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

		message.getMiner().tell(
			new DependencyMiner.WorkRequest(
				getContext().getSelf(),
				this.largeMessageProxy  // ✅ Include proxy reference!
			)
		);

		return this;
	}
}