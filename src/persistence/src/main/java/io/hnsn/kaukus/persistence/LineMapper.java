package io.hnsn.kaukus.persistence;

import java.io.IOException;
import java.nio.file.Path;

import io.hnsn.kaukus.persistence.SSTable.IndexTuple;

/*package*/ interface LineMapper {
    IndexTuple[] mapLines(float mappingRatio, Path filePath) throws IOException;
}