package com.google.cloud.firestore.migration.load;

import static org.junit.Assert.*;
import org.junit.Test;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import java.math.BigInteger;

public class ParallelVerificationPipelineTest {

    @Test
    public void testGetShardId() {
        Document doc1 = Document.newBuilder()
                .setName("projects/p1/databases/d1/documents/coll1/doc1")
                .build();
        Document doc2 = Document.newBuilder()
                .setName("projects/p2/databases/d2/documents/coll1/doc1")
                .build();
                
        int shard1 = ParallelVerificationPipeline.getShardId(doc1);
        int shard2 = ParallelVerificationPipeline.getShardId(doc2);
        
        assertEquals("Shards should be equal for same relative path", shard1, shard2);
        
        Document doc3 = Document.newBuilder()
                .setName("projects/p1/databases/d1/documents/coll2/doc1")
                .build();
        int shard3 = ParallelVerificationPipeline.getShardId(doc3);
        
        // They might be equal by chance, but usually they should be different.
        // We just check that it doesn't throw exception and works.
    }

    @Test
    public void testHashContent_consistent() {
        Document doc1 = Document.newBuilder()
                .putFields("field1", Value.newBuilder().setStringValue("value1").build())
                .build();
        Document doc2 = Document.newBuilder()
                .putFields("field1", Value.newBuilder().setStringValue("value1").build())
                .build();
                
        BigInteger hash1 = ParallelVerificationPipeline.hashContent(doc1);
        BigInteger hash2 = ParallelVerificationPipeline.hashContent(doc2);
        
        assertEquals("Hashes should be equal for same content", hash1, hash2);
    }

    @Test
    public void testHashContent_differsForDiffContent() {
        Document doc1 = Document.newBuilder()
                .putFields("field1", Value.newBuilder().setStringValue("value1").build())
                .build();
        Document doc2 = Document.newBuilder()
                .putFields("field1", Value.newBuilder().setStringValue("value2").build())
                .build();
                
        BigInteger hash1 = ParallelVerificationPipeline.hashContent(doc1);
        BigInteger hash2 = ParallelVerificationPipeline.hashContent(doc2);
        
        assertNotEquals("Hashes should differ for different content", hash1, hash2);
    }
}
