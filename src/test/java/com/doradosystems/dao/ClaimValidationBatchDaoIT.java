package com.doradosystems.dao;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.doradosystems.exception.NotFoundException;
import com.doradosystems.mis.dao.ClaimValidationBatchDao;
import com.doradosystems.mis.domain.ClaimValidationBatch;
import com.doradosystems.mis.domain.ClaimValidationBatch.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * 
 * @author Arthur Tolentino
 *
 */
@RunWith(SpringRunner.class)
@ContextConfiguration({ "/applicationContext-components.xml", "/applicationContext-property-placeholder.xml" })
public class ClaimValidationBatchDaoIT {
    
    @Autowired
    private ClaimValidationBatchDao dao;
    @Autowired
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;
    
    @Before
    public void cleanup() throws Exception {
        if (jdbcTemplate == null) {
            jdbcTemplate = new JdbcTemplate(dataSource);
        }
        jdbcTemplate.update("DELETE FROM mis_claim_validation_service.claim_validation_batch");
    }
    
    @Test
    public void add() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                Status.COMPLETE, 1L, "gcn", null, null);
        UUID id = dao.add(batch);
        
        ClaimValidationBatch result = dao.get(id);
        assertThat(result, is(notNullValue()));
    }
    
    @Test
    public void getById() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                Status.ERROR, 2L, "gcn", null, null);
        UUID id = dao.add(batch);
        
        ClaimValidationBatch result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(id, result.getClaimValidationBatchId());
        assertEquals(batch.getClientId(), result.getClientId());
        assertEquals(batch.getFilename(), result.getFilename());
        assertEquals(batch.getGlobalControlNumber(), result.getGlobalControlNumber());
        assertEquals(batch.getRunNumber(), result.getRunNumber());
        assertEquals(batch.getStatus(), result.getStatus());
        assertThat(result.getCreateDate(), is(notNullValue()));
        assertThat(result.getUpdatedDate(), is(notNullValue()));
    }
    
    @Test(expected = NotFoundException.class)
    public void getByIdThatDoesNotExist() throws Exception {
        dao.get(UUID.randomUUID());
    }
    
    @Test
    public void getByStatus() throws Exception{
        List<UUID> ids = new ArrayList<>();
        for(int i=0 ; i< 3 ; i++) {
            ids.add(dao.add(new ClaimValidationBatch(null, 1L, "filename",
                    Status.PROCESSING, 2L, "gcn", null, null)));
        }
        assertEquals(3, ids.size());
        List<ClaimValidationBatch> result = dao.getByStatus(Status.PROCESSING);
        assertEquals(3, result.size());
        result.forEach(claimValidationBatch -> {
            assertEquals(Status.PROCESSING, claimValidationBatch.getStatus());
        });
    }
    
    @Test
    public void getByStatusThatDoesNotExist() throws Exception {
        List<UUID> ids = new ArrayList<>();
        for(int i=0 ; i< 3 ; i++) {
            ids.add(dao.add(new ClaimValidationBatch(null, 1L, "filename",
                    Status.PROCESSING, 2L, "gcn", null, null)));
        }
        assertEquals(3, ids.size());
        
        List<ClaimValidationBatch> result = dao.getByStatus(Status.COMPLETE);
        assertEquals(0, result.size());
    }
    
    @Test
    public void getByStatusWithLimit() throws Exception{
        List<UUID> ids = new ArrayList<>();
        for(int i=0 ; i< 3 ; i++) {
            ids.add(dao.add(new ClaimValidationBatch(null, 1L, "filename",
                    Status.PROCESSING, 2L, "gcn", null, null)));
        }
        assertEquals(3, ids.size());
        List<ClaimValidationBatch> result = dao.getByStatusWithCreateTimeAscending(Status.PROCESSING, 1);
        assertEquals(1, result.size());
        result.forEach(claimValidationBatch -> {
            assertEquals(Status.PROCESSING, claimValidationBatch.getStatus());
            assertEquals(ids.get(0), claimValidationBatch.getClaimValidationBatchId());
        });
    }
    
    @Test
    public void count() throws Exception {
        List<UUID> ids = new ArrayList<>();
        for(int i=0 ; i< 3 ; i++) {
            ids.add(dao.add(new ClaimValidationBatch(null, 1L, "filename",
                    Status.LOADING, 2L, "gcn", null, null)));
        }
        assertEquals(3, ids.size());
        assertEquals(3, dao.countByStatus(Status.LOADING));
    }
    
    @Test
    public void updateStatus() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                Status.COMPLETE, 1L, "gcn", null, null);
        UUID id = dao.add(batch);
        
        ClaimValidationBatch result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(Status.COMPLETE, result.getStatus());
        
        int recordsUpdated = dao.updateStatus(id, Status.ERROR);
        assertEquals(1, recordsUpdated);
        result = dao.get(id);
        assertEquals(Status.ERROR, result.getStatus());
    }
    
    @Test(expected = NotFoundException.class)
    public void updateStatusWhereIdDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                Status.COMPLETE, 1L, "gcn", null, null);
        UUID id = dao.add(batch);
        
        ClaimValidationBatch result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(Status.COMPLETE, result.getStatus());
        
        dao.updateStatus(UUID.randomUUID(), Status.ERROR);
    }
    
    @Test
    public void updateStatusAndRunNumber() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                Status.COMPLETE, 1L, "gcn", null, null);
        UUID id = dao.add(batch);
        
        ClaimValidationBatch result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(Status.COMPLETE, result.getStatus());
        
        int recordsUpdated = dao.updateStatusAndRunNumber(id, Status.ERROR, 10L);
        assertEquals(1, recordsUpdated);
        result = dao.get(id);
        assertEquals(Status.ERROR, result.getStatus());
        assertEquals(10L, result.getRunNumber().longValue());
    }
    
    @Test(expected = NotFoundException.class)
    public void updateStatusAndRunNumberWhereIdDoesNotExist() throws Exception {
        ClaimValidationBatch batch = new ClaimValidationBatch(null, 1L, "filename",
                Status.COMPLETE, 1L, "gcn", null, null);
        UUID id = dao.add(batch);
        
        ClaimValidationBatch result = dao.get(id);
        assertThat(result, is(notNullValue()));
        assertEquals(Status.COMPLETE, result.getStatus());
        dao.updateStatusAndRunNumber(UUID.randomUUID(), Status.ERROR, 10L);
    }

}
