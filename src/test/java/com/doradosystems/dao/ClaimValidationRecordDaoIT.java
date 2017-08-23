package com.doradosystems.dao;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
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
import com.doradosystems.exception.NotFoundException;
import com.doradosystems.mis.dao.ClaimValidationBatchDao;
import com.doradosystems.mis.dao.ClaimValidationRecordDao;
import com.doradosystems.mis.domain.ClaimValidationBatch;
import com.doradosystems.mis.domain.ClaimValidationRecord;
import com.doradosystems.mis.domain.ClaimValidationRecord.Status;

/**
 * 
 * @author Arthur Tolentino
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration({ "/applicationContext-components.xml", "/applicationContext-property-placeholder.xml" })
public class ClaimValidationRecordDaoIT {
    
    @Autowired
    private ClaimValidationRecordDao dao;
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
        jdbcTemplate.update("DELETE FROM mis_claim_validation.claim_validation_record");
        jdbcTemplate.update("DELETE FROM mis_claim_validation.claim_validation_batch");
    }
    
    @Test
    public void add() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        ClaimValidationRecord result = dao.get(id);
        assertThat(result, is(notNullValue()));
    }
    
    @Test(expected = DataPersistenceException.class)
    public void addIllegalBatchId() throws Exception {
        ClaimValidationRecord record = new ClaimValidationRecord(null, UUID.randomUUID(), 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        dao.add(record);
    }
    
    @Test
    public void getById() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        ClaimValidationRecord result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(batchId, result.getClaimValidationBatchId());
        assertEquals(id, result.getClaimValidationRecordId());
        assertEquals(record.getClaimNumber(), result.getClaimNumber());
        assertEquals(record.getRecord(), result.getRecord());
        assertEquals(record.getRunNumber(), result.getRunNumber());
        assertEquals(record.getStatus(), result.getStatus());
        assertThat(result.getCreateDate(), is(notNullValue()));
        assertThat(result.getUpdatedDate(), is(notNullValue()));
    }
    
    @Test(expected = NotFoundException.class)
    public void getByIdThatDoesNotExist() throws Exception {
        dao.get(UUID.randomUUID());
    }
    
    @Test
    public void getByBatchIAndRunNumber() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        List<ClaimValidationRecord> results = dao.getByBatchIdAndRunNumber(batchId, record.getRunNumber());
        assertEquals(1, results.size());
        ClaimValidationRecord result = results.get(0);
        assertThat(result, is(notNullValue()));
        assertEquals(batchId, result.getClaimValidationBatchId());
        assertEquals(id, result.getClaimValidationRecordId());
        assertEquals(record.getClaimNumber(), result.getClaimNumber());
        assertEquals(record.getRecord(), result.getRecord());
        assertEquals(record.getRunNumber(), result.getRunNumber());
        assertEquals(record.getStatus(), result.getStatus());
        assertThat(result.getCreateDate(), is(notNullValue()));
        assertThat(result.getUpdatedDate(), is(notNullValue()));
    }
    
    @Test
    public void getByBatchIAndRunNumberWhereBatchIdDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        dao.add(record);
        List<ClaimValidationRecord> results = dao.getByBatchIdAndRunNumber(UUID.randomUUID(), record.getRunNumber());
        assertEquals(0, results.size());
    }
    
    @Test
    public void getByBatchIAndRunNumberWhereRunNumberDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        dao.add(record);
        List<ClaimValidationRecord> results = dao.getByBatchIdAndRunNumber(batchId, 0L);
        assertEquals(0, results.size());
    }
    
    @Test
    public void getByBatchIAndRunNumberWithLimit() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        List<UUID> ids = new ArrayList<>();
        for(int i=0; i<3 ; i++) {
            ids.add(dao.add(new ClaimValidationRecord(null, batchId, 1L, Status.INCOMPLETE, "claimNumber",
                "record", null, null)));
        }
        assertEquals(3, ids.size());
        
        List<ClaimValidationRecord> results = dao.getByBatchIdAndRunNumberOrderByUpdateDateDescending(batchId, 1L, 1);
        assertEquals(1, results.size());
        assertEquals(ids.get(2), results.get(0).getClaimValidationRecordId());
    }
    
    @Test
    public void countByBatchIdRunNumberStatus() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        List<UUID> ids = new ArrayList<>();
        for(int i=0; i<3 ; i++) {
            ids.add(dao.add(new ClaimValidationRecord(null, batchId, 1L, Status.INCOMPLETE, "claimNumber",
                "record", null, null)));
        }
        assertEquals(3, ids.size());
        
        int count = dao.countByBatchIdAndRunNumberAndStatus(batchId, 1L, Status.INCOMPLETE);
        assertEquals(3, count);
    }
    
    @Test
    public void countByBatchIdRunNumberStatusWithVaryingStatuses() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        
        List<UUID> ids = new ArrayList<>();
        ids.add(dao.add(new ClaimValidationRecord(null, batchId, 1L, Status.INCOMPLETE, "claimNumber",
                "record", null, null)));
        ids.add(dao.add(new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null)));
        ids.add(dao.add(new ClaimValidationRecord(null, batchId, 1L, Status.PENDING, "claimNumber",
                "record", null, null)));
        ids.add(dao.add(new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null)));
        assertEquals(4, ids.size());
        
        assertEquals(1, dao.countByBatchIdAndRunNumberAndStatus(batchId, 1L, Status.INCOMPLETE));
        assertEquals(2, dao.countByBatchIdAndRunNumberAndStatus(batchId, 1L, Status.COMPLETE));
        assertEquals(1, dao.countByBatchIdAndRunNumberAndStatus(batchId, 1L, Status.PENDING));
    }
    
    @Test
    public void updateStatus() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        ClaimValidationRecord result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(record.getStatus(), result.getStatus());
        
        int updated = dao.updateStatus(id, Status.PENDING);
        assertEquals(1, updated);
        result = dao.get(id);
        assertEquals(Status.PENDING, result.getStatus());
    }
    
    @Test(expected = NotFoundException.class)
    public void updateStatusWhereIdDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        ClaimValidationRecord result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(record.getStatus(), result.getStatus());
        
        dao.updateStatus(UUID.randomUUID(), Status.PENDING);
    }
    
    @Test
    public void updateStatusAndRunNumber() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        ClaimValidationRecord result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(record.getStatus(), result.getStatus());
        assertEquals(record.getRunNumber(), result.getRunNumber());
        
        int updated = dao.updateStatusAndRunNumber(batchId, record.getRunNumber(), record.getStatus(), 20L, Status.INCOMPLETE);
        assertEquals(1, updated);
        result = dao.get(id);
        assertEquals(Status.INCOMPLETE, result.getStatus());
        assertEquals(20L, result.getRunNumber().longValue());
    }
    
    @Test
    public void updateStatusAndRunNumberWhereStatusDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        ClaimValidationRecord result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(record.getStatus(), result.getStatus());
        assertEquals(record.getRunNumber(), result.getRunNumber());
        
        int updated = dao.updateStatusAndRunNumber(batchId, record.getRunNumber(), Status.PENDING, 20L, Status.INCOMPLETE);
        assertEquals(0, updated);
    }
    
    @Test
    public void updateStatusAndRunNumberWhereRunNumberDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                com.doradosystems.mis.domain.ClaimValidationBatch.Status.COMPLETE, 1L, "gcn", null, null);
        UUID batchId = batchDao.add(batch);
        ClaimValidationRecord record = new ClaimValidationRecord(null, batchId, 1L, Status.COMPLETE, "claimNumber",
                "record", null, null);
        
        UUID id = dao.add(record);
        ClaimValidationRecord result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(record.getStatus(), result.getStatus());
        assertEquals(record.getRunNumber(), result.getRunNumber());
        
        int updated = dao.updateStatusAndRunNumber(batchId, System.currentTimeMillis(), record.getStatus(), 20L, Status.INCOMPLETE);
        assertEquals(0, updated);
    }

}
