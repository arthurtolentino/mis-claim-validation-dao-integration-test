package com.doradosystems.dao;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.doradosystems.exception.DataPersistenceException;
import com.doradosystems.mis.dao.ClaimValidationBatchDao;
import com.doradosystems.mis.dao.ClaimValidationRecordDao;
import com.doradosystems.mis.dao.ClaimValidationResponseDao;
import com.doradosystems.mis.domain.ClaimValidationBatch;
import com.doradosystems.mis.domain.ClaimValidationRecord;
import com.doradosystems.mis.domain.ClaimValidationResponse;
import com.doradosystems.mis.domain.ClaimValidationResponseIdentifier;

/**
 * 
 * @author Arthur Tolentino
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration({ "/applicationContext-components.xml", "/applicationContext-property-placeholder.xml" })
public class ClaimValidationResponseDaoIT {

    @Autowired
    private ClaimValidationResponseDao dao;
    @Autowired
    private ClaimValidationRecordDao recordDao;
    @Autowired
    private ClaimValidationBatchDao batchDao;
    @Autowired
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;
    
    @Before
    @After
    public void cleanup() throws Exception {
        if (jdbcTemplate == null) {
            jdbcTemplate = new JdbcTemplate(dataSource);
        }
        jdbcTemplate.update("DELETE FROM mis_claim_validation_service.claim_validation_response");
        jdbcTemplate.update("DELETE FROM mis_claim_validation_service.claim_validation_record");
        jdbcTemplate.update("DELETE FROM mis_claim_validation_service.claim_validation_batch");
    }
    
    @Test
    public void add() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename", ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, ClaimValidationRecord.Status.COMPLETE, "claimNumber",
                "record", null, null);
        UUID recordId = recordDao.add(record);
        
        ClaimValidationResponse response = new ClaimValidationResponse(null, batchId, 1L,
                ClaimValidationResponse.Status.COMPLETE, "foo", "bar", null, null, recordId);
        UUID id = dao.add(response);
        int count = jdbcTemplate.queryForObject(
                "select count(*) from mis_claim_validation_service.claim_validation_response where claim_validation_response_id = ?", new Object[] { id },
                Integer.class);
        assertEquals(1, count);
    }
    
    @Test(expected = DataPersistenceException.class)
    public void addBatchIdDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename", ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, ClaimValidationRecord.Status.COMPLETE, "claimNumber",
                "record", null, null);
        UUID recordId = recordDao.add(record);
        
        ClaimValidationResponse response = new ClaimValidationResponse(null, UUID.randomUUID(), 1L,
                ClaimValidationResponse.Status.COMPLETE, "foo", "bar", null, null, recordId);
        dao.add(response);
    }
    
    @Test(expected = DataPersistenceException.class)
    public void addRecordIdDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename", ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, ClaimValidationRecord.Status.COMPLETE, "claimNumber",
                "record", null, null);
        recordDao.add(record);
        
        ClaimValidationResponse response = new ClaimValidationResponse(null, batchId, 1L,
                ClaimValidationResponse.Status.COMPLETE, "foo", "bar", null, null, UUID.randomUUID());
        dao.add(response);
    }
    
    @Test
    public void getByBatchIdAndRunNumber() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename", ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, ClaimValidationRecord.Status.COMPLETE, "claimNumber",
                "record", null, null);
        UUID recordId = recordDao.add(record);
        
        Long runNumber = 1L;
        ClaimValidationResponse response1 = new ClaimValidationResponse(null, batchId, runNumber,
                ClaimValidationResponse.Status.COMPLETE, "foo", "bar", null, null, recordId);
        ClaimValidationResponse response2 = new ClaimValidationResponse(null, batchId, runNumber,
                ClaimValidationResponse.Status.PENDING, "foo", "bar", null, null, recordId);
        UUID id1 = dao.add(response1);
        UUID id2 = dao.add(response2);
        int count = jdbcTemplate.queryForObject(
                "select count(*) from mis_claim_validation_service.claim_validation_response where claim_validation_response_id in (?, ?)", new Object[] { id1, id2 },
                Integer.class);
        assertEquals(2, count);
        
        List<ClaimValidationResponse> results = dao.get(batchId, 1L);
        assertEquals(2, results.size());
        assertClaimValidationResponseEquals(response1, results.get(0), id1);
        assertClaimValidationResponseEquals(response2, results.get(1), id2);
    }
    
    @Test
    public void getPendingResponseIdentifiers() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename", ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, ClaimValidationRecord.Status.COMPLETE, "claimNumber",
                "record", null, null);
        UUID recordId = recordDao.add(record);
        
        ClaimValidationResponse response = new ClaimValidationResponse(null, batchId, 1L,
                ClaimValidationResponse.Status.PENDING, "foo", "bar", null, null, recordId);
        UUID id = dao.add(response);
        int count = jdbcTemplate.queryForObject(
                "select count(*) from mis_claim_validation_service.claim_validation_response where claim_validation_response_id = ?", new Object[] { id },
                Integer.class);
        assertEquals(1, count);
        
        List<ClaimValidationResponseIdentifier> results = dao.getPendingResponseIdentifiers();
        assertEquals(1, results.size());
    }
    
    @Test
    public void getPendingResponseIdentifiersWithNoAvailableRecords() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename", ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, ClaimValidationRecord.Status.COMPLETE, "claimNumber",
                "record", null, null);
        UUID recordId = recordDao.add(record);
        
        ClaimValidationResponse response = new ClaimValidationResponse(null, batchId, 1L,
                ClaimValidationResponse.Status.COMPLETE, "foo", "bar", null, null, recordId);
        UUID id = dao.add(response);
        int count = jdbcTemplate.queryForObject(
                "select count(*) from mis_claim_validation_service.claim_validation_response where claim_validation_response_id = ?", new Object[] { id },
                Integer.class);
        assertEquals(1, count);
        
        List<ClaimValidationResponseIdentifier> results = dao.getPendingResponseIdentifiers();
        assertEquals(0, results.size());
    }
    
    @Test
    public void updateStatus() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename", ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, ClaimValidationRecord.Status.COMPLETE, "claimNumber",
                "record", null, null);
        UUID recordId = recordDao.add(record);
        
        ClaimValidationResponse response = new ClaimValidationResponse(null, batchId, 1L,
                ClaimValidationResponse.Status.PENDING, "foo", "bar", null, null, recordId);
        UUID id = dao.add(response);
        int count = jdbcTemplate.queryForObject(
                "select count(*) from mis_claim_validation_service.claim_validation_response where claim_validation_response_id = ? and status = ?::mis_claim_validation_service.status",
                new Object[] { id, ClaimValidationResponse.Status.PENDING.toString() }, Integer.class);
        assertEquals(1, count);
        
        int updated = dao.updateStatus(batchId, response.getRunNumber(), ClaimValidationResponse.Status.COMPLETE);
        assertEquals(1, updated);
        count = jdbcTemplate.queryForObject(
                "select count(*) from mis_claim_validation_service.claim_validation_response where claim_validation_response_id = ? and status = ?::mis_claim_validation_service.status",
                new Object[] { id, ClaimValidationResponse.Status.COMPLETE.toString() }, Integer.class);
        assertEquals(1, count);
    }
    
    private void assertClaimValidationResponseEquals(ClaimValidationResponse expected, ClaimValidationResponse actual, UUID id) {
        assertEquals(id, actual.getClaimValidationResponseId());
        assertEquals(expected.getClaimValidationBatchId(), actual.getClaimValidationBatchId());
        assertEquals(expected.getClaimNumber(), actual.getClaimNumber());
        assertEquals(expected.getClaimValidationRecordId(), actual.getClaimValidationRecordId());
        assertEquals(expected.getResponse(), actual.getResponse());
        assertEquals(expected.getRunNumber(), actual.getRunNumber());
        assertEquals(expected.getStatus(), actual.getStatus());
        assertThat(actual.getCreateDate(), is(notNullValue()));
    }
    
}
