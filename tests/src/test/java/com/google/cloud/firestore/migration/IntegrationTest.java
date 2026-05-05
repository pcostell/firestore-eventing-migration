package com.google.cloud.firestore.migration;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.DocumentSnapshot;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {
    private static String srcProject;
    private static String dstProject;
    private static String srcDb;
    private static String dstDb;
    private static Firestore srcFirestore;
    private static Firestore dstFirestore;
    private static Process migrateProcess;
    
    @BeforeClass
    public static void setUp() throws Exception {
        srcProject = System.getenv("SOURCE_PROJECT");
        if (srcProject == null || srcProject.isEmpty()) {
            srcProject = System.getenv("PROJECT_ID");
        }
        dstProject = System.getenv("DEST_PROJECT");
        if (dstProject == null || dstProject.isEmpty()) {
            dstProject = System.getenv("PROJECT_ID");
        }
        
        if (srcProject == null || srcProject.isEmpty()) {
            throw new RuntimeException("PROJECT_ID or SOURCE_PROJECT environment variable is required.");
        }
        
        long timestamp = System.currentTimeMillis() / 1000;
        srcDb = "test-src-" + timestamp;
        dstDb = "test-dst-" + timestamp;
        
        System.out.println("Creating source database: " + srcDb + " in project " + srcProject);
        runCommand("gcloud", "firestore", "databases", "create", 
                "--project=" + srcProject, "--database=" + srcDb, 
                "--type=firestore-native", "--location=us-central1", "--quiet");

        System.out.println("Creating destination database: " + dstDb + " in project " + dstProject);
        String destEdition = System.getenv("DEST_EDITION");
        if (destEdition == null || destEdition.isEmpty()) {
            destEdition = "ENTERPRISE";
        }

        if ("ENTERPRISE".equalsIgnoreCase(destEdition)) {
            runCommand("gcloud", "firestore", "databases", "create", 
                    "--project=" + dstProject, "--database=" + dstDb, 
                    "--edition=enterprise", "--enable-firestore-data-access", "--location=us-central1", "--quiet");
        } else {
            runCommand("gcloud", "firestore", "databases", "create", 
                    "--project=" + dstProject, "--database=" + dstDb, 
                    "--type=firestore-native", "--location=us-central1", "--quiet");
        }

        // Grant permissions
        System.out.println("Granting cross-project permissions...");
        String dstProjectNumber = runCommandWithOutput("gcloud", "projects", "describe", dstProject, "--format=value(projectNumber)");
        String computeSa = dstProjectNumber + "-compute@developer.gserviceaccount.com";
        
        runCommand("gcloud", "projects", "add-iam-policy-binding", srcProject, 
                "--member=serviceAccount:" + computeSa, 
                "--role=roles/datastore.viewer", 
                "--condition=None", "--quiet");

        // Grant reverse permission for Source SA on Dest Project
        System.out.println("Granting reverse cross-project permissions...");
        String srcProjectNumber = runCommandWithOutput("gcloud", "projects", "describe", srcProject, "--format=value(projectNumber)");
        String srcComputeSa = srcProjectNumber + "-compute@developer.gserviceaccount.com";
        
        runCommand("gcloud", "projects", "add-iam-policy-binding", dstProject, 
                "--member=serviceAccount:" + srcComputeSa, 
                "--role=roles/datastore.user", 
                "--condition=None", "--quiet");

        srcFirestore = FirestoreOptions.newBuilder().setProjectId(srcProject).setDatabaseId(srcDb).build().getService();
        dstFirestore = FirestoreOptions.newBuilder().setProjectId(dstProject).setDatabaseId(dstDb).build().getService();
    }
    
    @AfterClass
    public static void tearDown() {
        System.out.println("Cleaning up...");
        if (migrateProcess != null && migrateProcess.isAlive()) {
            migrateProcess.destroyForcibly();
        }
        
        // Remove permissions
        System.out.println("Removing cross-project permissions...");
        try {
            String dstProjectNumber = runCommandWithOutput("gcloud", "projects", "describe", dstProject, "--format=value(projectNumber)");
            String computeSa = dstProjectNumber + "-compute@developer.gserviceaccount.com";
            runCommand("gcloud", "projects", "remove-iam-policy-binding", srcProject, 
                    "--member=serviceAccount:" + computeSa, 
                    "--role=roles/datastore.viewer", "--condition=None", "--quiet");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            String srcProjectNumber = runCommandWithOutput("gcloud", "projects", "describe", srcProject, "--format=value(projectNumber)");
            String srcComputeSa = srcProjectNumber + "-compute@developer.gserviceaccount.com";
            runCommand("gcloud", "projects", "remove-iam-policy-binding", dstProject, 
                    "--member=serviceAccount:" + srcComputeSa, 
                    "--role=roles/datastore.user", "--condition=None", "--quiet");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            runCommand("./migrate.sh", "cleanup", "--source-project", srcProject, "--dest-project", dstProject, "--dest-db", dstDb);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            runCommand("gcloud", "firestore", "databases", "delete", 
                    "--project=" + srcProject, "--database=" + srcDb, "--quiet");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            runCommand("gcloud", "firestore", "databases", "delete", 
                    "--project=" + dstProject, "--database=" + dstDb, "--quiet");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void runCommand(String... command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        // Since test runs in /tests directory, execute from root:
        pb.directory(new File(".."));
        pb.environment().put("NON_INTERACTIVE", "true");
        pb.inheritIO();
        Process p = pb.start();
        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Command failed with exit code " + exitCode + ": " + String.join(" ", command));
        }
    }

    private static String runCommandWithOutput(String... command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes()).trim();
        int exitCode = p.waitFor();
        return out;
    }

    @Test
    public void testMigration() throws Exception {
        System.out.println("Writing Setup DocA...");
        Map<String, Object> docA = new HashMap<>();
        docA.put("stage", "setup");
        docA.put("time", System.currentTimeMillis());
        srcFirestore.collection("test_data").document("DocA").set(docA).get();
        
        Map<String, Object> docDeleteBefore = new HashMap<>();
        docDeleteBefore.put("stage", "setup_delete");
        srcFirestore.collection("test_data").document("DocDeleteBefore").set(docDeleteBefore).get();
        
        System.out.println("Running migrate.sh run...");
        ProcessBuilder migratePb = new ProcessBuilder("./migrate.sh", "run", 
            "--source-project", srcProject, 
            "--source-db", srcDb, 
            "--dest-project", dstProject, 
            "--dest-db", dstDb, 
            "--workers", "1");
        migratePb.environment().put("NON_INTERACTIVE", "true");
        migratePb.directory(new File(".."));
        migratePb.redirectErrorStream(true);
        migratePb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        migrateProcess = migratePb.start();
        
        System.out.println("Waiting for Dataflow Job ID...");
        String jobId = "";
        while (jobId.isEmpty()) {
            // Check jobs
            String output = runCommandWithOutput("bash", "-c", 
                "gcloud dataflow jobs list --project=" + dstProject + " --region=us-central1 --format='value(id,name,state)' | grep firestoremigrationpipeline | grep -i running | awk '{print $1}' | head -n 1");
            jobId = output;
            if (jobId.isEmpty()) {
                Thread.sleep(5000);
            }
            // Check if process failed early
            if (!migrateProcess.isAlive() && migrateProcess.exitValue() != 0) {
                throw new RuntimeException("migrate.sh failed unexpectedly.");
            }
        }
        
        System.out.println("Found Dataflow Job ID: " + jobId);
        System.out.println("Sleeping 30s for pipeline scale up...");
        Thread.sleep(30000);
        
        System.out.println("Writing DocB (during migration)...");
        Map<String, Object> docB = new HashMap<>();
        docB.put("stage", "during");
        docB.put("time", System.currentTimeMillis());
        srcFirestore.collection("test_data").document("DocB").set(docB).get();
        
        // Update DocA to verify HWM latest update overrides setup update
        srcFirestore.collection("test_data").document("DocA").update("updatedDuring", true).get();
        srcFirestore.collection("test_data").document("DocDeleteBefore").delete().get();
        
        System.out.println("Polling Dataflow Job status for completion...");
        while (true) {
            String state = runCommandWithOutput("gcloud", "dataflow", "jobs", "describe", jobId, 
                "--project=" + dstProject, "--region=us-central1", "--format=value(currentState)");
            
            System.out.println("Dataflow Job state: " + state);
            if ("JOB_STATE_DONE".equals(state)) {
                break;
            } else if ("JOB_STATE_FAILED".equals(state) || "JOB_STATE_CANCELLED".equals(state)) {
                throw new RuntimeException("Dataflow job failed or cancelled.");
            }
            Thread.sleep(10000);
        }
        
        System.out.println("Writing DocC (after migration via Live Sink)...");
        Map<String, Object> docC = new HashMap<>();
        docC.put("stage", "after");
        docC.put("time", System.currentTimeMillis());
        srcFirestore.collection("test_data").document("DocC").set(docC).get();
        
        System.out.println("Writing and Deleting DocDeleteAfter (after migration)...");
        Map<String, Object> docDeleteAfter = new HashMap<>();
        docDeleteAfter.put("stage", "after_delete");
        srcFirestore.collection("test_data").document("DocDeleteAfter").set(docDeleteAfter).get();
        srcFirestore.collection("test_data").document("DocDeleteAfter").delete().get();
        
        System.out.println("Sleeping 30s to allow Eventarc propagation...");
        Thread.sleep(30000);
        
        System.out.println("Verifying records in Destination...");
        DocumentSnapshot dstA = dstFirestore.collection("test_data").document("DocA").get().get();
        DocumentSnapshot dstB = dstFirestore.collection("test_data").document("DocB").get().get();
        DocumentSnapshot dstC = dstFirestore.collection("test_data").document("DocC").get().get();
        
        assertTrue("DocA should exist", dstA.exists());
        assertEquals(true, dstA.getBoolean("updatedDuring"));
        
        assertTrue("DocB should exist", dstB.exists());
        assertEquals("during", dstB.getString("stage"));
        
        assertTrue("DocC should exist", dstC.exists());
        assertEquals("after", dstC.getString("stage"));
        
        DocumentSnapshot dstDeleteBefore = dstFirestore.collection("test_data").document("DocDeleteBefore").get().get();
        DocumentSnapshot dstDeleteAfter = dstFirestore.collection("test_data").document("DocDeleteAfter").get().get();
        
        assertTrue("DocDeleteBefore should NOT exist", !dstDeleteBefore.exists());
        assertTrue("DocDeleteAfter should NOT exist", !dstDeleteAfter.exists());
        
        System.out.println("Integration Test Verified Successfully!");
    }
}
