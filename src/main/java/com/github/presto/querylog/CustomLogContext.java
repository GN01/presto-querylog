package com.github.presto.querylog;

import io.prestosql.spi.eventlistener.QueryCompletedEvent;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Archon  2019/10/29
 * @since 0.3
 */
public class CustomLogContext {

    private static final long MB_BYTES = 1_048_576;
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Metadata metadata;

    private Statistics statistics;

    private Context context;

    private List<InputMetaData> inputMetaDataList;

    private String createTime;
    private String startTime;
    private String endTime;

    public CustomLogContext parse(QueryCompletedEvent event, int trackEventCompletedQueryLength) {
        Metadata metadata = new Metadata();
        metadata.setQueryId(event.getMetadata().getQueryId());
        String query = event.getMetadata().getQuery().trim();
        if (trackEventCompletedQueryLength != -1 && query.length() > trackEventCompletedQueryLength) {
            StringBuilder sb = new StringBuilder(trackEventCompletedQueryLength);
            sb.append(query, 0, (trackEventCompletedQueryLength-4)/2);
            sb.append("....");
            sb.append(query, query.length()-(trackEventCompletedQueryLength-4)/2, query.length());
            metadata.setQuery(sb.toString());
        } else {
            metadata.setQuery(query);
        }
        this.setMetadata(metadata);

        Statistics statistics = new Statistics();
        statistics.setCpuSecond(event.getStatistics().getCpuTime().getSeconds());
        statistics.setWallSecond(event.getStatistics().getWallTime().getSeconds());
        statistics.setQueuedSecond(event.getStatistics().getQueuedTime().getSeconds());
        event.getStatistics().getAnalysisTime().ifPresent(d -> statistics.setAnalysisSecond(d.getSeconds()));
        statistics.setPeakUserMemoryMB(event.getStatistics().getPeakUserMemoryBytes()/MB_BYTES);
        statistics.setPeakTotalNonRevocableMemoryMB(event.getStatistics().getPeakTotalNonRevocableMemoryBytes()/MB_BYTES);
        statistics.setPeakTaskUserMemoryMB(event.getStatistics().getPeakTaskUserMemory()/MB_BYTES);
        statistics.setPeakTaskTotalMemoryMB(event.getStatistics().getPeakTaskTotalMemory()/MB_BYTES);
        statistics.setPhysicalInputMB(event.getStatistics().getPhysicalInputBytes()/MB_BYTES);
        statistics.setPhysicalInputRows(event.getStatistics().getPhysicalInputRows());
        statistics.setInternalNetworkMB(event.getStatistics().getInternalNetworkBytes()/MB_BYTES);
        statistics.setInternalNetworkRows(event.getStatistics().getInternalNetworkRows());
        statistics.setTotalMB(event.getStatistics().getTotalBytes()/MB_BYTES);
        statistics.setTotalRows(event.getStatistics().getTotalRows());
        statistics.setOutputMB(event.getStatistics().getOutputBytes()/MB_BYTES);
        statistics.setOutputRows(event.getStatistics().getOutputRows());
        statistics.setWrittenMB(event.getStatistics().getWrittenBytes()/MB_BYTES);
        statistics.setWrittenRows(event.getStatistics().getWrittenRows());
        this.setStatistics(statistics);

        Context context = new Context();
        context.setUser(event.getContext().getUser());
        this.setContext(context);

        List<InputMetaData> inputMetaDataList = new ArrayList<>(event.getIoMetadata().getInputs().size());
        event.getIoMetadata().getInputs().forEach(i -> {
            InputMetaData inputMetaData = new InputMetaData();
            inputMetaData.setCatalogName(i.getCatalogName());
            inputMetaData.setSchema(i.getSchema());
            inputMetaData.setTable(i.getTable());
            inputMetaDataList.add(inputMetaData);
        });
        this.setInputMetaDataList(inputMetaDataList);

        this.setCreateTime(LocalDateTime.ofInstant(event.getCreateTime(), ZoneId.of("UTC")).format(DATE_TIME_FORMATTER));
        this.setStartTime(LocalDateTime.ofInstant(event.getExecutionStartTime(), ZoneId.of("UTC")).format(DATE_TIME_FORMATTER));
        this.setEndTime(LocalDateTime.ofInstant(event.getEndTime(), ZoneId.of("UTC")).format(DATE_TIME_FORMATTER));

        return this;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public List<InputMetaData> getInputMetaDataList() {
        return inputMetaDataList;
    }

    public void setInputMetaDataList(List<InputMetaData> inputMetaDataList) {
        this.inputMetaDataList = inputMetaDataList;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    static class Metadata {
        private String queryId;
        private String query;

        public String getQueryId() {
            return queryId;
        }

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }
    }

    static class Statistics {
        private long cpuSecond;
        private long wallSecond;
        private long queuedSecond;
        private long analysisSecond;
        private long peakUserMemoryMB;
        private long peakTotalNonRevocableMemoryMB;
        private long peakTaskUserMemoryMB;
        private long peakTaskTotalMemoryMB;
        private long physicalInputMB;
        private long physicalInputRows;
        private long internalNetworkMB;
        private long internalNetworkRows;
        private long totalMB;
        private long totalRows;
        private long outputMB;
        private long outputRows;
        private long writtenMB;
        private long writtenRows;

        public long getCpuSecond() {
            return cpuSecond;
        }

        public void setCpuSecond(long cpuSecond) {
            this.cpuSecond = cpuSecond;
        }

        public long getWallSecond() {
            return wallSecond;
        }

        public void setWallSecond(long wallSecond) {
            this.wallSecond = wallSecond;
        }

        public long getQueuedSecond() {
            return queuedSecond;
        }

        public void setQueuedSecond(long queuedSecond) {
            this.queuedSecond = queuedSecond;
        }

        public long getAnalysisSecond() {
            return analysisSecond;
        }

        public void setAnalysisSecond(long analysisSecond) {
            this.analysisSecond = analysisSecond;
        }

        public long getPeakUserMemoryMB() {
            return peakUserMemoryMB;
        }

        public void setPeakUserMemoryMB(long peakUserMemoryMB) {
            this.peakUserMemoryMB = peakUserMemoryMB;
        }

        public long getPeakTotalNonRevocableMemoryMB() {
            return peakTotalNonRevocableMemoryMB;
        }

        public void setPeakTotalNonRevocableMemoryMB(long peakTotalNonRevocableMemoryMB) {
            this.peakTotalNonRevocableMemoryMB = peakTotalNonRevocableMemoryMB;
        }

        public long getPeakTaskUserMemoryMB() {
            return peakTaskUserMemoryMB;
        }

        public void setPeakTaskUserMemoryMB(long peakTaskUserMemoryMB) {
            this.peakTaskUserMemoryMB = peakTaskUserMemoryMB;
        }

        public long getPeakTaskTotalMemoryMB() {
            return peakTaskTotalMemoryMB;
        }

        public void setPeakTaskTotalMemoryMB(long peakTaskTotalMemoryMB) {
            this.peakTaskTotalMemoryMB = peakTaskTotalMemoryMB;
        }

        public long getPhysicalInputMB() {
            return physicalInputMB;
        }

        public void setPhysicalInputMB(long physicalInputMB) {
            this.physicalInputMB = physicalInputMB;
        }

        public long getPhysicalInputRows() {
            return physicalInputRows;
        }

        public void setPhysicalInputRows(long physicalInputRows) {
            this.physicalInputRows = physicalInputRows;
        }

        public long getInternalNetworkMB() {
            return internalNetworkMB;
        }

        public void setInternalNetworkMB(long internalNetworkMB) {
            this.internalNetworkMB = internalNetworkMB;
        }

        public long getInternalNetworkRows() {
            return internalNetworkRows;
        }

        public void setInternalNetworkRows(long internalNetworkRows) {
            this.internalNetworkRows = internalNetworkRows;
        }

        public long getTotalMB() {
            return totalMB;
        }

        public void setTotalMB(long totalMB) {
            this.totalMB = totalMB;
        }

        public long getTotalRows() {
            return totalRows;
        }

        public void setTotalRows(long totalRows) {
            this.totalRows = totalRows;
        }

        public long getOutputMB() {
            return outputMB;
        }

        public void setOutputMB(long outputMB) {
            this.outputMB = outputMB;
        }

        public long getOutputRows() {
            return outputRows;
        }

        public void setOutputRows(long outputRows) {
            this.outputRows = outputRows;
        }

        public long getWrittenMB() {
            return writtenMB;
        }

        public void setWrittenMB(long writtenMB) {
            this.writtenMB = writtenMB;
        }

        public long getWrittenRows() {
            return writtenRows;
        }

        public void setWrittenRows(long writtenRows) {
            this.writtenRows = writtenRows;
        }
    }

    static class Context {
        private String user;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }
    }

    static class InputMetaData {
        private String catalogName;
        private String schema;
        private String table;

        public String getCatalogName() {
            return catalogName;
        }

        public void setCatalogName(String catalogName) {
            this.catalogName = catalogName;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }
    }
}
