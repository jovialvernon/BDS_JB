package de.ddm.actors.profiling;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class InputReader extends AbstractBehavior<InputReader.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	public static class ReadHeaderMessage implements Message {

		private ActorRef<DependencyMiner.Message> replyTo;
	}



	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	public static class ReadBatchMessage implements Message {

		private ActorRef<DependencyMiner.Message> replyTo;
		private int batchSize;
	}




	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "inputReader";

	public static Behavior<Message> create(final int id, final File inputFile) {
		return Behaviors.setup(context -> new InputReader(context, id, inputFile));
	}

	private InputReader(ActorContext<Message> context, final int id, final File inputFile) throws IOException, CsvValidationException {
		super(context);
		this.id = id;
		this.reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
		this.header = InputConfigurationSingleton.get().getHeader(inputFile);
		
		if (InputConfigurationSingleton.get().isFileHasHeader())
			this.reader.readNext();
	}

	/////////////////
	// Actor State //
	/////////////////

	private final int id;
	private final CSVReader reader;
	private final String[] header;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReadHeaderMessage.class, this::handle)
				.onMessage(ReadBatchMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}


	private Behavior<Message> handle(ReadBatchMessage message) throws IOException, CsvValidationException {
		List<String[]> batch = new ArrayList<>(message.getBatchSize());
		for (int i = 0; i < message.getBatchSize(); i++) {
			String[] line = this.reader.readNext();
			if (line == null)
				break;
			batch.add(line);
		}

		message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, batch));
		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.reader.close();
		return this;
	}

	private Behavior<Message> handle(ReadHeaderMessage message) {
		// Send header
		message.getReplyTo().tell(new DependencyMiner.HeaderMessage(this.id, this.header));
		
		// Load and send all column data
		try {
			loadAndSendAllColumns(message.getReplyTo());
		} catch (Exception e) {
			getContext().getLog().error("Failed to load columns: {}", e.getMessage());
		}
		
		return this;
	}

	private void loadAndSendAllColumns(ActorRef<DependencyMiner.Message> replyTo) 
			throws IOException, CsvValidationException {
		
		File inputFile = InputConfigurationSingleton.get().getInputFiles()[this.id];
		
		// Create array of sets, one for each column
		java.util.Set<String>[] columnSets = new java.util.HashSet[this.header.length];
		for (int i = 0; i < this.header.length; i++) {
			columnSets[i] = new java.util.HashSet<>();
		}
		
		// Create new reader to avoid interfering with batch reading
		CSVReader dataReader = InputConfigurationSingleton.get().createCSVReader(inputFile);
		
		if (InputConfigurationSingleton.get().isFileHasHeader()) {
			dataReader.readNext(); // Skip header
		}
		
		// Read all rows and populate column sets
		String[] line;
		while ((line = dataReader.readNext()) != null) {
			for (int colIdx = 0; colIdx < line.length && colIdx < this.header.length; colIdx++) {
				columnSets[colIdx].add(line[colIdx]);
			}
		}	
		
		dataReader.close();
		
		// Send each column to the miner
		for (int colIdx = 0; colIdx < this.header.length; colIdx++) {
			getContext().getLog().info("Sending column {} from file {} ({} unique values)", 
				colIdx, this.id, columnSets[colIdx].size());	
			
			replyTo.tell(new DependencyMiner.ColumnDataMessage(this.id, colIdx, columnSets[colIdx]));
		}
	}



	
}
