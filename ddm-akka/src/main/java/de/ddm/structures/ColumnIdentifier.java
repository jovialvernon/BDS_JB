package de.ddm.structures;

import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Identifies a specific column in a specific file.
 * Used for attribute partitioning where each worker owns a subset of columns.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ColumnIdentifier implements AkkaSerializable {
    private static final long serialVersionUID = 1L;
    
    private int fileId;
    private int columnIndex;
    
    @Override
    public String toString() {
        return fileId + ":" + columnIndex;
    }
    
    /**
     * Parse a ColumnIdentifier from string format "fileId:columnIndex"
     */
    public static ColumnIdentifier fromString(String s) {
        String[] parts = s.split(":");
        return new ColumnIdentifier(
            Integer.parseInt(parts[0]), 
            Integer.parseInt(parts[1])
        );
    }
}
