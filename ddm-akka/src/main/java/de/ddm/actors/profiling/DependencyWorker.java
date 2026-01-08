package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
//import lombok.AllArgsConstructor;
//import lombok.Getter;
//import lombok.NoArgsConstructor;


import java.util.Set;

import de.ddm.singletons.InputConfigurationSingleton;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;

import java.io.File;


public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

//	@Getter
//	@NoArgsConstructor
//	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private final Receptionist.Listing listing;

		public ReceptionistListingMessage(Receptionist.Listing listing) {
			this.listing = listing;
		}

		public Receptionist.Listing getListing() {
			return listing;
		}
	}



	//	@Getter
//	@NoArgsConstructor
//	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;

		private int dependentFileId;
		private int dependentColumnIndex;
		private int referencedFileId;
		private int referencedColumnIndex;

		private ActorRef<DependencyMiner.Message> miner;
		private ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;

		public TaskMessage(
				int dependentFileId,
				int dependentColumnIndex,
				int referencedFileId,
				int referencedColumnIndex,
				ActorRef<DependencyMiner.Message> miner,
				ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy
		) {
			this.dependentFileId = dependentFileId;
			this.dependentColumnIndex = dependentColumnIndex;
			this.referencedFileId = referencedFileId;
			this.referencedColumnIndex = referencedColumnIndex;
			this.miner = miner;
			this.dependencyMinerLargeMessageProxy = dependencyMinerLargeMessageProxy;
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
	}


//	@NoArgsConstructor
//	@AllArgsConstructor
//	public static class RequestWorkMessage implements Message {
//		private static final long serialVersionUID = 1L;
//		ActorRef<DependencyMiner.Message> dependencyMiner;
//	}


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

	private final java.util.Map<String, Set<String>> columnCache = new java.util.HashMap<>();

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
			dependencyMiner.tell(new DependencyMiner.RequestWorkMessage(this.getContext().getSelf()));

		}

		return this;
	}

	private Set<String> loadColumn(File file, int columnIndex) throws Exception {

		Set<String> values = new HashSet<>();

		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {

			String line;
			boolean headerSkipped = false;

			while ((line = reader.readLine()) != null) {

				if (!headerSkipped) { // skip header
					headerSkipped = true;
					continue;
				}

				String[] parts = line.split(";");
				if (columnIndex < parts.length) {
					values.add(parts[columnIndex]);
				}
			}
		}

		return values;
	}

	private Set<String> loadColumnCached(File file, int fileId, int columnIndex) throws Exception {
		String key = fileId + ":" + columnIndex;
		Set<String> cached = columnCache.get(key);
		if (cached != null) {
			return cached;
		}
		Set<String> values = loadColumn(file, columnIndex);
		columnCache.put(key, values);
		return values;
	}


//	private Behavior<Message> handle(TaskMessage message) {
////		this.getContext().getLog().info("Working!");
////		// I should probably know how to solve this task, but for now I just pretend some work...
////
////		int result = message.getTask();
////		long time = System.currentTimeMillis();
////		Random rand = new Random();
////		int runtime = (rand.nextInt(2) + 2) * 1000;
////		while (System.currentTimeMillis() - time < runtime)
////			result = ((int) Math.abs(Math.sqrt(result)) * result) % 1334525;
////
////		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
////		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));
////
////		return this;
//
//
//		this.getContext().getLog().info("Worker idle – waiting for real task.");
//		return this;
//	}

	private Behavior<Message> handle(TaskMessage message) {

		boolean violated = false;

		try {
			File dependentFile =
					InputConfigurationSingleton.get().getInputFiles()[message.getDependentFileId()];
			File referencedFile =
					InputConfigurationSingleton.get().getInputFiles()[message.getReferencedFileId()];


			Set<String> dependentValues = loadColumnCached(dependentFile, message.getDependentFileId(), message.getDependentColumnIndex());
			Set<String> referencedValues = loadColumnCached(referencedFile, message.getReferencedFileId(), message.getReferencedColumnIndex());

			for (String value : dependentValues) {
				if (!referencedValues.contains(value)) {
					violated = true;
					break;
				}
			}

		} catch (Exception e) {
			violated = true; // fail-safe
		}

		DependencyMiner.UnaryIndResult result =
				new DependencyMiner.UnaryIndResult(
						message.getDependentFileId(),
						message.getDependentColumnIndex(),
						message.getReferencedFileId(),
						message.getReferencedColumnIndex(),
						violated
				);

		// send result back via LargeMessageProxy
		this.largeMessageProxy.tell(
				new LargeMessageProxy.SendMessage(
						result,
						message.getDependencyMinerLargeMessageProxy()
				)
		);

		// IMPORTANT: ask miner for next task
		message.getMiner().tell(
				new DependencyMiner.RequestWorkMessage(getContext().getSelf())
		);

//		getContext().getSystem().receptionist().tell(
//				new DependencyMiner.RequestWorkMessage(getContext().getSelf())
//		);

		return this;
	}
}