package com.google.cloud.firestore.migration;

import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Transaction;
import com.google.firestore.v1.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FirestoreSinkTest {

    @Mock private Firestore db;
    @Mock private Transaction transaction;
    @Mock private DocumentReference destDocRef;
    @Mock private CollectionReference shadowJournalCollection;
    @Mock private DocumentReference journalRef;
    @Mock private DocumentSnapshot journalSnap;
    @Mock private MigrationMetrics metrics;

    private FirestoreSink sink;

    @Before
    public void setUp() throws Exception {
        sink = new FirestoreSink(db, metrics);

        when(db.document(anyString())).thenReturn(destDocRef);
        when(db.collection("_ShadowJournal")).thenReturn(shadowJournalCollection);
        when(shadowJournalCollection.document(anyString())).thenReturn(journalRef);

        when(transaction.get(journalRef)).thenReturn(ApiFutures.immediateFuture(journalSnap));

        when(db.runTransaction(any())).thenAnswer(invocation -> {
            Transaction.Function<Void> function = invocation.getArgument(0);
            return ApiFutures.immediateFuture(function.updateCallback(transaction));
        });
    }

    private Document createIncomingDoc(long seconds) {
        return Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(seconds).build())
                .build();
    }

    @Test
    public void testIncomingTimestampAfterExistingHwm_shouldWrite() throws Exception {
        // Setup: existing HWM is at epoch 1000
        when(journalSnap.exists()).thenReturn(true);
        when(journalSnap.contains("hwm")).thenReturn(true);
        when(journalSnap.getDate("hwm")).thenReturn(Date.from(Instant.ofEpochSecond(1000)));

        // Incoming is at epoch 1001
        Document doc = createIncomingDoc(1001);

        // Execute
        sink.process(doc);

        // Verification
        verify(transaction).set(eq(destDocRef), any(Map.class));
        
        ArgumentCaptor<Map<String, Object>> journalCaptor = ArgumentCaptor.forClass(Map.class);
        verify(transaction).set(eq(journalRef), journalCaptor.capture());
        
        Map<String, Object> journalData = journalCaptor.getValue();
        assertEquals(Date.from(Instant.ofEpochSecond(1001)), journalData.get("hwm"));
        verify(metrics).recordOperation(MigrationMetrics.Operation.WRITE);
    }

    @Test
    public void testIncomingTimestampBeforeExistingHwm_shouldSkip() throws Exception {
        // Setup: existing HWM is at epoch 1000
        when(journalSnap.exists()).thenReturn(true);
        when(journalSnap.contains("hwm")).thenReturn(true);
        when(journalSnap.getDate("hwm")).thenReturn(Date.from(Instant.ofEpochSecond(1000)));

        // Incoming is at epoch 999
        Document doc = createIncomingDoc(999);

        // Execute
        sink.process(doc);

        // Verification: Neither destination doc nor journal should be updated
        verify(transaction, never()).set(any(DocumentReference.class), any(Map.class));
        verify(metrics).recordOperation(MigrationMetrics.Operation.NOOP_WRITE);
    }

    @Test
    public void testIncomingTimestampEqualsExistingHwm_shouldSkip() throws Exception {
        // Setup: existing HWM is at epoch 1000
        when(journalSnap.exists()).thenReturn(true);
        when(journalSnap.contains("hwm")).thenReturn(true);
        when(journalSnap.getDate("hwm")).thenReturn(Date.from(Instant.ofEpochSecond(1000)));

        // Incoming is at epoch 1000
        Document doc = createIncomingDoc(1000);

        // Execute
        sink.process(doc);

        // Verification: Neither destination doc nor journal should be updated
        verify(transaction, never()).set(any(DocumentReference.class), any(Map.class));
        verify(metrics).recordOperation(MigrationMetrics.Operation.NOOP_WRITE);
    }

    @Test
    public void testNoExistingHwm_shouldWrite() throws Exception {
        // Setup: journal document doesn't exist
        when(journalSnap.exists()).thenReturn(false);

        // Incoming is at epoch 100
        Document doc = createIncomingDoc(100);

        // Execute
        sink.process(doc);

        // Verification
        verify(transaction).set(eq(destDocRef), any(Map.class));
        verify(transaction).set(eq(journalRef), any(Map.class));
        verify(metrics).recordOperation(MigrationMetrics.Operation.WRITE);
    }

    @Test
    public void testDelete_IncomingTimestampAfterExistingHwm_shouldDelete() throws Exception {
        // Setup: existing HWM is at epoch 1000
        when(journalSnap.exists()).thenReturn(true);
        when(journalSnap.contains("hwm")).thenReturn(true);
        when(journalSnap.getDate("hwm")).thenReturn(Date.from(Instant.ofEpochSecond(1000)));

        // Incoming delete is at epoch 1001
        String fullPath = "projects/test-project/databases/(default)/documents/test_data/doc1";
        Instant commitTime = Instant.ofEpochSecond(1001);

        // Execute
        sink.process(fullPath, commitTime, null);

        // Verification
        verify(transaction).delete(eq(destDocRef));
        
        ArgumentCaptor<Map<String, Object>> journalCaptor = ArgumentCaptor.forClass(Map.class);
        verify(transaction).set(eq(journalRef), journalCaptor.capture());
        
        Map<String, Object> journalData = journalCaptor.getValue();
        assertEquals(Date.from(Instant.ofEpochSecond(1001)), journalData.get("hwm"));
        verify(metrics).recordOperation(MigrationMetrics.Operation.DELETE);
    }

    @Test
    public void testDelete_IncomingTimestampBeforeExistingHwm_shouldSkip() throws Exception {
        // Setup: existing HWM is at epoch 1000
        when(journalSnap.exists()).thenReturn(true);
        when(journalSnap.contains("hwm")).thenReturn(true);
        when(journalSnap.getDate("hwm")).thenReturn(Date.from(Instant.ofEpochSecond(1000)));

        // Incoming delete is at epoch 999
        String fullPath = "projects/test-project/databases/(default)/documents/test_data/doc1";
        Instant commitTime = Instant.ofEpochSecond(999);

        // Execute
        sink.process(fullPath, commitTime, null);

        // Verification: Neither destination doc nor journal should be updated
        verify(transaction, never()).delete(any(DocumentReference.class));
        verify(transaction, never()).set(any(DocumentReference.class), any(Map.class));
        verify(metrics).recordOperation(MigrationMetrics.Operation.NOOP_DELETE);
    }

    @Test
    public void testDelete_NoExistingHwm_shouldDelete() throws Exception {
        // Setup: journal document doesn't exist
        when(journalSnap.exists()).thenReturn(false);

        // Incoming delete is at epoch 100
        String fullPath = "projects/test-project/databases/(default)/documents/test_data/doc1";
        Instant commitTime = Instant.ofEpochSecond(100);

        // Execute
        sink.process(fullPath, commitTime, null);

        // Verification
        verify(transaction).delete(eq(destDocRef));
        
        ArgumentCaptor<Map<String, Object>> journalCaptor = ArgumentCaptor.forClass(Map.class);
        verify(transaction).set(eq(journalRef), journalCaptor.capture());
        
        Map<String, Object> journalData = journalCaptor.getValue();
        assertEquals(Date.from(Instant.ofEpochSecond(100)), journalData.get("hwm"));
        verify(metrics).recordOperation(MigrationMetrics.Operation.DELETE);
    }
}
