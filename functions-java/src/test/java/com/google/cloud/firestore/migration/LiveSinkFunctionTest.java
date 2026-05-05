package com.google.cloud.firestore.migration;

import com.google.cloud.firestore.Firestore;
import com.google.events.cloud.firestore.v1.DocumentEventData;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiveSinkFunctionTest {

    @Mock
    private Firestore db;

    @Mock
    private FirestoreSink sink;

    @Mock
    private CloudEvent cloudEvent;

    @Mock
    private CloudEventData cloudEventData;

    private LiveSinkFunction function;

    @Before
    public void setUp() {
        function = new LiveSinkFunction(db, sink);
    }

    @Test
    public void testAcceptValidEvent() throws Exception {
        // Build a mock com.google.events.cloud.firestore.v1.Document
        com.google.events.cloud.firestore.v1.Document eventsDoc = com.google.events.cloud.firestore.v1.Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(1000).build())
                .build();

        // Wrap it in DocumentEventData
        DocumentEventData eventData = DocumentEventData.newBuilder()
                .setValue(eventsDoc)
                .build();

        // Setup the mocked CloudEvent
        when(cloudEvent.getType()).thenReturn("google.cloud.firestore.document.v1.written");
        when(cloudEvent.getData()).thenReturn(cloudEventData);
        when(cloudEventData.toBytes()).thenReturn(eventData.toByteArray());

        // Execute
        function.accept(cloudEvent);

        // Verify the extracted document was passed to FirestoreSink
        ArgumentCaptor<com.google.firestore.v1.Document> docCaptor = ArgumentCaptor.forClass(com.google.firestore.v1.Document.class);
        verify(sink).process(docCaptor.capture());

        com.google.firestore.v1.Document capturedDoc = docCaptor.getValue();
        assertNotNull(capturedDoc);
        assertEquals("projects/test-project/databases/(default)/documents/test_data/doc1", capturedDoc.getName());
        assertEquals(1000, capturedDoc.getUpdateTime().getSeconds());
    }

    @Test
    public void testAcceptDeleteEvent() throws Exception {
        com.google.events.cloud.firestore.v1.Document oldDoc = com.google.events.cloud.firestore.v1.Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                .setUpdateTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(2000).setNanos(500).build())
                .build();

        DocumentEventData eventData = DocumentEventData.newBuilder()
                .setOldValue(oldDoc)
                .build();

        when(cloudEvent.getType()).thenReturn("google.cloud.firestore.document.v1.deleted");
        when(cloudEvent.getData()).thenReturn(cloudEventData);
        when(cloudEventData.toBytes()).thenReturn(eventData.toByteArray());

        function.accept(cloudEvent);

        java.time.Instant expectedDeleteTime = java.time.Instant.ofEpochSecond(2000, 501); // 500 nanos + 1 nano
        verify(sink).process(
            org.mockito.ArgumentMatchers.eq("projects/test-project/databases/(default)/documents/test_data/doc1"),
            org.mockito.ArgumentMatchers.eq(expectedDeleteTime),
            org.mockito.ArgumentMatchers.isNull()
        );
    }

    @Test(expected = RuntimeException.class)
    public void testAcceptValidEvent_missingUpdateTime() throws Exception {
        com.google.events.cloud.firestore.v1.Document eventsDoc = com.google.events.cloud.firestore.v1.Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                // No update time set
                .build();

        DocumentEventData eventData = DocumentEventData.newBuilder()
                .setValue(eventsDoc)
                .build();

        when(cloudEvent.getType()).thenReturn("google.cloud.firestore.document.v1.written");
        when(cloudEvent.getData()).thenReturn(cloudEventData);
        when(cloudEventData.toBytes()).thenReturn(eventData.toByteArray());

        function.accept(cloudEvent);
    }

    @Test(expected = RuntimeException.class)
    public void testAcceptDeleteEvent_missingUpdateTime() throws Exception {
        com.google.events.cloud.firestore.v1.Document oldDoc = com.google.events.cloud.firestore.v1.Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                // No update time set
                .build();

        DocumentEventData eventData = DocumentEventData.newBuilder()
                .setOldValue(oldDoc)
                .build();

        when(cloudEvent.getType()).thenReturn("google.cloud.firestore.document.v1.deleted");
        when(cloudEvent.getData()).thenReturn(cloudEventData);
        when(cloudEventData.toBytes()).thenReturn(eventData.toByteArray());

        function.accept(cloudEvent);
    }
}
