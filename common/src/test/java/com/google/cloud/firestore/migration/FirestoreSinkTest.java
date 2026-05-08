package com.google.cloud.firestore.migration;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BeginTransactionRequest;
import com.google.firestore.v1.BeginTransactionResponse;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CommitResponse;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.RollbackRequest;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.protobuf.ByteString;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FirestoreSinkTest {

    @Mock private FirestoreClient client;
    @Mock private MigrationMetrics metrics;
    @Mock private BeginTransactionResponse beginResponse;
    @Mock private ServerStream<BatchGetDocumentsResponse> serverStream;
    @Mock private BatchGetDocumentsResponse batchGetResponse;
    @Mock private CommitResponse commitResponse;
    @Mock private ServerStreamingCallable<BatchGetDocumentsRequest, BatchGetDocumentsResponse> callable;

    private FirestoreSink sink;
    private ByteString transactionId = ByteString.copyFromUtf8("test-transaction");

    @Before
    public void setUp() throws Exception {
        sink = new FirestoreSink(client, metrics, "test-project", "(default)");

        when(client.beginTransaction(any(BeginTransactionRequest.class))).thenReturn(beginResponse);
        when(beginResponse.getTransaction()).thenReturn(transactionId);
        
        com.google.protobuf.Timestamp commitTimestamp = com.google.protobuf.Timestamp.newBuilder().setSeconds(2000).build();
        when(client.commit(any(CommitRequest.class))).thenReturn(commitResponse);
        when(commitResponse.getCommitTime()).thenReturn(commitTimestamp);
    }

    private Document createIncomingDoc(long seconds) {
        return Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(seconds).build())
                .build();
    }

    private void mockJournalResponse(boolean found, long seconds) {
        if (found) {
            String path1 = "projects/test-project/databases/(default)/documents/test_data/doc1";
            String journalPath1 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path1);
            Document journalDoc = Document.newBuilder()
                .setName(journalPath1)
                .putFields("hwm", Value.newBuilder().setTimestampValue(
                    com.google.protobuf.Timestamp.newBuilder().setSeconds(seconds).build()
                ).build())
                .build();
            when(batchGetResponse.hasFound()).thenReturn(true);
            when(batchGetResponse.getFound()).thenReturn(journalDoc);
        } else {
            when(batchGetResponse.hasFound()).thenReturn(false);
        }
        
        Iterator<BatchGetDocumentsResponse> iterator = Collections.singletonList(batchGetResponse).iterator();
        when(serverStream.iterator()).thenReturn(iterator);
        lenient().when(client.batchGetDocumentsCallable()).thenReturn(callable);
        lenient().when(callable.call(any(BatchGetDocumentsRequest.class))).thenReturn(serverStream);
    }

    @Test
    public void testIncomingTimestampAfterExistingHwm_shouldWrite() throws Exception {
        mockJournalResponse(true, 1000);
        Document doc = createIncomingDoc(1001);

        sink.process(doc);

        ArgumentCaptor<CommitRequest> commitCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(client).commit(commitCaptor.capture());
        
        CommitRequest commitRequest = commitCaptor.getValue();
        assertEquals(2, commitRequest.getWritesCount());
        assertEquals(doc, commitRequest.getWrites(0).getUpdate());
        
        Value hwmVal = commitRequest.getWrites(1).getUpdate().getFieldsMap().get("hwm");
        assertEquals(1001, hwmVal.getTimestampValue().getSeconds());
        
        verify(metrics).recordOperation(MigrationMetrics.Operation.WRITE);
    }

    @Test
    public void testIncomingTimestampBeforeExistingHwm_shouldSkip() throws Exception {
        mockJournalResponse(true, 1000);
        Document doc = createIncomingDoc(999);

        sink.process(doc);

        verify(client, never()).commit(any(CommitRequest.class));
        verify(client).rollback(any(RollbackRequest.class));
        verify(metrics).recordOperation(MigrationMetrics.Operation.NOOP_WRITE);
    }

    @Test
    public void testIncomingTimestampEqualsExistingHwm_shouldSkip() throws Exception {
        mockJournalResponse(true, 1000);
        Document doc = createIncomingDoc(1000);

        sink.process(doc);

        verify(client, never()).commit(any(CommitRequest.class));
        verify(client).rollback(any(RollbackRequest.class));
        verify(metrics).recordOperation(MigrationMetrics.Operation.NOOP_WRITE);
    }

    @Test
    public void testNoExistingHwm_shouldWrite() throws Exception {
        mockJournalResponse(false, 0);
        Document doc = createIncomingDoc(100);

        sink.process(doc);

        ArgumentCaptor<CommitRequest> commitCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(client).commit(commitCaptor.capture());
        
        CommitRequest commitRequest = commitCaptor.getValue();
        assertEquals(2, commitRequest.getWritesCount());
        assertEquals(doc, commitRequest.getWrites(0).getUpdate());
        
        verify(metrics).recordOperation(MigrationMetrics.Operation.WRITE);
    }

    @Test
    public void testDelete_IncomingTimestampAfterExistingHwm_shouldDelete() throws Exception {
        mockJournalResponse(true, 1000);
        String fullPath = "projects/test-project/databases/(default)/documents/test_data/doc1";
        Timestamp commitTime = Timestamp.ofTimeSecondsAndNanos(1001, 0);

        sink.process(fullPath, commitTime, null);

        ArgumentCaptor<CommitRequest> commitCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(client).commit(commitCaptor.capture());
        
        CommitRequest commitRequest = commitCaptor.getValue();
        assertEquals(2, commitRequest.getWritesCount());
        assertEquals(fullPath, commitRequest.getWrites(0).getDelete());
        
        verify(metrics).recordOperation(MigrationMetrics.Operation.DELETE);
    }

    @Test
    public void testDelete_IncomingTimestampBeforeExistingHwm_shouldSkip() throws Exception {
        mockJournalResponse(true, 1000);
        String fullPath = "projects/test-project/databases/(default)/documents/test_data/doc1";
        Timestamp commitTime = Timestamp.ofTimeSecondsAndNanos(999, 0);

        sink.process(fullPath, commitTime, null);

        verify(client, never()).commit(any(CommitRequest.class));
        verify(client).rollback(any(RollbackRequest.class));
        verify(metrics).recordOperation(MigrationMetrics.Operation.NOOP_DELETE);
    }

    @Test
    public void testDelete_NoExistingHwm_shouldDelete() throws Exception {
        mockJournalResponse(false, 0);
        String fullPath = "projects/test-project/databases/(default)/documents/test_data/doc1";
        Timestamp commitTime = Timestamp.ofTimeSecondsAndNanos(100, 0);

        sink.process(fullPath, commitTime, null);

        ArgumentCaptor<CommitRequest> commitCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(client).commit(commitCaptor.capture());
        
        CommitRequest commitRequest = commitCaptor.getValue();
        assertEquals(2, commitRequest.getWritesCount());
        assertEquals(fullPath, commitRequest.getWrites(0).getDelete());
        
        verify(metrics).recordOperation(MigrationMetrics.Operation.DELETE);
    }

    private void mockBatchJournalResponses(String[] paths, boolean[] founds, long[] seconds) {
        java.util.List<BatchGetDocumentsResponse> responses = new java.util.ArrayList<>();
        for (int i = 0; i < paths.length; i++) {
            BatchGetDocumentsResponse resp = mock(BatchGetDocumentsResponse.class);
            if (founds[i]) {
                Document journalDoc = Document.newBuilder()
                    .setName(paths[i])
                    .putFields("hwm", Value.newBuilder().setTimestampValue(
                        com.google.protobuf.Timestamp.newBuilder().setSeconds(seconds[i]).build()
                    ).build())
                    .build();
                when(resp.hasFound()).thenReturn(true);
                when(resp.getFound()).thenReturn(journalDoc);
            } else {
                when(resp.hasFound()).thenReturn(false);
            }
            responses.add(resp);
        }
        
        Iterator<BatchGetDocumentsResponse> iterator = responses.iterator();
        when(serverStream.iterator()).thenReturn(iterator);
        lenient().when(client.batchGetDocumentsCallable()).thenReturn(callable);
        lenient().when(callable.call(any(BatchGetDocumentsRequest.class))).thenReturn(serverStream);
    }

    private FirestoreSink.Mutation toMutation(Document doc) {
        com.google.protobuf.Timestamp updateTime = doc.getUpdateTime();
        Timestamp commitTime = Timestamp.ofTimeSecondsAndNanos(updateTime.getSeconds(), updateTime.getNanos());
        return new FirestoreSink.Mutation(doc.getName(), commitTime, doc);
    }

    @Test
    public void testBatch_allNeedUpdate_shouldCommitAll() throws Exception {
        String path1 = "projects/test-project/databases/(default)/documents/test_data/doc1";
        String path2 = "projects/test-project/databases/(default)/documents/test_data/doc2";
        String journalPath1 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path1);
        String journalPath2 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path2);
        
        mockBatchJournalResponses(
            new String[]{journalPath1, journalPath2},
            new boolean[]{true, true},
            new long[]{1000, 2000}
        );

        Document doc1 = Document.newBuilder()
            .setName(path1)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(1001).build())
            .build();
            
        Document doc2 = Document.newBuilder()
            .setName(path2)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(2001).build())
            .build();

        sink.processBatch(java.util.Arrays.asList(toMutation(doc1), toMutation(doc2)));

        ArgumentCaptor<CommitRequest> commitCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(client).commit(commitCaptor.capture());
        
        CommitRequest commitRequest = commitCaptor.getValue();
        assertEquals(4, commitRequest.getWritesCount()); // 2 docs + 2 journals
        
        verify(metrics, times(2)).recordOperation(MigrationMetrics.Operation.WRITE);
    }

    @Test
    public void testBatch_someNeedUpdate_shouldCommitPartial() throws Exception {
        String path1 = "projects/test-project/databases/(default)/documents/test_data/doc1";
        String path2 = "projects/test-project/databases/(default)/documents/test_data/doc2";
        String journalPath1 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path1);
        String journalPath2 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path2);
        
        mockBatchJournalResponses(
            new String[]{journalPath1, journalPath2},
            new boolean[]{true, true},
            new long[]{1000, 2000}
        );

        Document doc1 = Document.newBuilder()
            .setName(path1)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(1001).build()) // Needs update (1001 > 1000)
            .build();
            
        Document doc2 = Document.newBuilder()
            .setName(path2)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(1999).build()) // Noop (1999 < 2000)
            .build();

        sink.processBatch(java.util.Arrays.asList(toMutation(doc1), toMutation(doc2)));

        ArgumentCaptor<CommitRequest> commitCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(client).commit(commitCaptor.capture());
        
        CommitRequest commitRequest = commitCaptor.getValue();
        assertEquals(2, commitRequest.getWritesCount()); // Only 1 doc + 1 journal
        assertEquals(path1, commitRequest.getWrites(0).getUpdate().getName());
        
        verify(metrics).recordOperation(MigrationMetrics.Operation.WRITE);
        verify(metrics).recordOperation(MigrationMetrics.Operation.NOOP_WRITE);
    }

    @Test
    public void testBatch_noneNeedUpdate_shouldRollback() throws Exception {
        String path1 = "projects/test-project/databases/(default)/documents/test_data/doc1";
        String path2 = "projects/test-project/databases/(default)/documents/test_data/doc2";
        String journalPath1 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path1);
        String journalPath2 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path2);
        
        mockBatchJournalResponses(
            new String[]{journalPath1, journalPath2},
            new boolean[]{true, true},
            new long[]{1000, 2000}
        );

        Document doc1 = Document.newBuilder()
            .setName(path1)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(999).build()) // Noop
            .build();
            
        Document doc2 = Document.newBuilder()
            .setName(path2)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(1999).build()) // Noop
            .build();

        sink.processBatch(java.util.Arrays.asList(toMutation(doc1), toMutation(doc2)));

        verify(client, never()).commit(any(CommitRequest.class));
        verify(client).rollback(any(RollbackRequest.class));
        verify(metrics, times(2)).recordOperation(MigrationMetrics.Operation.NOOP_WRITE);
    }

    @Test
    public void testBatch_variousFieldTypes_shouldWriteCorrectly() throws Exception {
        String path1 = "projects/test-project/databases/(default)/documents/test_data/doc1";
        String journalPath1 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path1);
        
        mockBatchJournalResponses(
            new String[]{journalPath1},
            new boolean[]{false},
            new long[]{0}
        );

        // Build Map Value (Nested!)
        com.google.firestore.v1.MapValue nestedMap = com.google.firestore.v1.MapValue.newBuilder()
            .putFields("nested_string", Value.newBuilder().setStringValue("hello").build())
            .putFields("nested_int", Value.newBuilder().setIntegerValue(42).build())
            .build();

        // Build Array Value
        com.google.firestore.v1.ArrayValue arrayVal = com.google.firestore.v1.ArrayValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("a").build())
            .addValues(Value.newBuilder().setStringValue("b").build())
            .build();

        Document doc1 = Document.newBuilder()
            .setName(path1)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(100).build())
            .putFields("null_field", Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build())
            .putFields("bool_field", Value.newBuilder().setBooleanValue(true).build())
            .putFields("int_field", Value.newBuilder().setIntegerValue(123).build())
            .putFields("double_field", Value.newBuilder().setDoubleValue(3.14).build())
            .putFields("string_field", Value.newBuilder().setStringValue("test").build())
            .putFields("timestamp_field", Value.newBuilder().setTimestampValue(com.google.protobuf.Timestamp.newBuilder().setSeconds(1000).build()).build())
            .putFields("bytes_field", Value.newBuilder().setBytesValue(ByteString.copyFromUtf8("bytes")).build())
            .putFields("ref_field", Value.newBuilder().setReferenceValue("projects/test-project/databases/(default)/documents/other/doc").build())
            .putFields("geo_field", Value.newBuilder().setGeoPointValue(com.google.type.LatLng.newBuilder().setLatitude(37.7).setLongitude(-122.4).build()).build())
            .putFields("array_field", Value.newBuilder().setArrayValue(arrayVal).build())
            .putFields("map_field", Value.newBuilder().setMapValue(nestedMap).build())
            .build();

        sink.processBatch(java.util.Collections.singletonList(toMutation(doc1)));

        ArgumentCaptor<CommitRequest> commitCaptor = ArgumentCaptor.forClass(CommitRequest.class);
        verify(client).commit(commitCaptor.capture());
        
        CommitRequest commitRequest = commitCaptor.getValue();
        assertEquals(2, commitRequest.getWritesCount());
        
        Document committedDoc = commitRequest.getWrites(0).getUpdate();
        assertEquals(doc1, committedDoc); // Verifies all fields are fully preserved!
    }

    @Test
    public void testBatch_retryOnContention_shouldSucceedOnSecondTry() throws Exception {
        String path1 = "projects/test-project/databases/(default)/documents/test_data/doc1";
        String journalPath1 = "projects/test-project/databases/(default)/documents/_ShadowJournal/" + sink.hashDocName(path1);
        
        mockBatchJournalResponses(
            new String[]{journalPath1},
            new boolean[]{false},
            new long[]{0}
        );

        Document doc1 = Document.newBuilder()
            .setName(path1)
            .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(100).build())
            .build();

        CommitResponse commitResponse = CommitResponse.newBuilder()
            .setCommitTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(200).build())
            .build();
            
        when(client.commit(any(CommitRequest.class)))
            .thenThrow(new io.grpc.StatusRuntimeException(io.grpc.Status.ABORTED.withDescription("Contention!")))
            .thenReturn(commitResponse);

        sink.processBatch(java.util.Collections.singletonList(toMutation(doc1)));

        verify(client, times(2)).beginTransaction(any(BeginTransactionRequest.class));
        verify(client, times(2)).commit(any(CommitRequest.class));
        
        verify(metrics, times(2)).recordOperation(MigrationMetrics.Operation.WRITE);
    }
}
