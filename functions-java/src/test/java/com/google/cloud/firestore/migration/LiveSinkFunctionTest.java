/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.firestore.migration;

import com.google.cloud.Timestamp;
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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class LiveSinkFunctionTest {

    @Mock
    private FirestoreSink sink;

    @Mock
    private CloudEvent cloudEvent;

    @Mock
    private CloudEventData cloudEventData;

    private LiveSinkFunction function;

    @Before
    public void setUp() {
        function = new LiveSinkFunction(sink);
    }

    @Test
    public void testAcceptValidEvent() throws Exception {
        // Build a mock com.google.events.cloud.firestore.v1.Document
        com.google.events.cloud.firestore.v1.Document eventsDoc = com.google.events.cloud.firestore.v1.Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                .build();

        // Wrap it in DocumentEventData
        DocumentEventData eventData = DocumentEventData.newBuilder()
                .setValue(eventsDoc)
                .build();

        // Setup the mocked CloudEvent with commit time of 1000s
        OffsetDateTime eventTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(1000), ZoneOffset.UTC);
        when(cloudEvent.getTime()).thenReturn(eventTime);
        when(cloudEvent.getType()).thenReturn("google.cloud.firestore.document.v1.written");
        when(cloudEvent.getData()).thenReturn(cloudEventData);
        when(cloudEventData.toBytes()).thenReturn(eventData.toByteArray());

        // Execute
        function.accept(cloudEvent);

        // Verify the extracted document was passed to FirestoreSink with exact commitTime
        verify(sink).process(
            eq("projects/test-project/databases/(default)/documents/test_data/doc1"),
            eq(Timestamp.ofTimeSecondsAndNanos(1000, 0)),
            any(com.google.firestore.v1.Document.class)
        );
    }

    @Test
    public void testAcceptDeleteEvent() throws Exception {
        com.google.events.cloud.firestore.v1.Document oldDoc = com.google.events.cloud.firestore.v1.Document.newBuilder()
                .setName("projects/test-project/databases/(default)/documents/test_data/doc1")
                .build();

        DocumentEventData eventData = DocumentEventData.newBuilder()
                .setOldValue(oldDoc)
                .build();

        // Setup the mocked CloudEvent with commit time of 2000s, 500ns
        OffsetDateTime eventTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(2000, 500), ZoneOffset.UTC);
        when(cloudEvent.getTime()).thenReturn(eventTime);
        when(cloudEvent.getType()).thenReturn("google.cloud.firestore.document.v1.deleted");
        when(cloudEvent.getData()).thenReturn(cloudEventData);
        when(cloudEventData.toBytes()).thenReturn(eventData.toByteArray());

        // Execute
        function.accept(cloudEvent);

        // Verify the deletion was passed to FirestoreSink with exact commitTime (no +1 nano needed!)
        Timestamp expectedDeleteTime = Timestamp.ofTimeSecondsAndNanos(2000, 500);
        verify(sink).process(
            eq("projects/test-project/databases/(default)/documents/test_data/doc1"),
            eq(expectedDeleteTime),
            org.mockito.ArgumentMatchers.isNull()
        );
    }
}
