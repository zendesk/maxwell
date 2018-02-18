package com.zendesk.maxwell;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import static java.util.logging.Level.INFO;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigRequest;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigResponse;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigResponse.NodeInfo;
import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellMysqlStatus;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.BinlogConnectorFileReader;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;

public class MaxwellFileReader {
	private static String className = MaxwellFileReader.class.getName();
	private static Logger logger = Logger.getLogger(className);
	
	//static public List<String> downloadList = new ArrayList<String>();	
	protected MaxwellConfig config;
	protected MaxwellContext context;
	protected Replicator replicator;
	
	public MaxwellFileReader(MaxwellConfig config) throws SQLException {
		this(new MaxwellContext(config));
	}
	
	protected MaxwellFileReader(MaxwellContext context) throws SQLException {
		this.config = context.getConfig();
		this.context = context;
		this.context.probeConnections();
	}
	
	protected ArrayList<String> getFileList() throws Exception {
		ArrayList<String> fl = new ArrayList<String>();
		File folder = new File(config.filePath);
		File[] files = folder.listFiles();
		for (File file : files) {
			String fn = file.getName();
			logger.log(INFO, "binlog filename:" + fn);
			fl.add(fn);
			fl.sort(null);
		}
		return fl;
	}
	

	private void downLoadBinlog(ArrayList<String> downloadList) throws Exception {
		logger.log(INFO,"Start to retrieve down link via Aliyun SDK");
		String host = config.replicationMysql.host;
		String iid = (host.split("\\."))[0];//instance id
		if (iid.endsWith("o")) {
			int l = iid.length();
			iid = iid.substring(0, l-1);
		}
		//date calculate
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		//end date.
		String endDate = formatter.format(date);
		
		Calendar cl = Calendar.getInstance();
		cl.setTime(date);
        cl.add(Calendar.DATE, -7);
        String startDate = formatter.format(cl.getTime());
        logger.log(INFO,"Date range:" + startDate + endDate);
                
		DefaultProfile profile = DefaultProfile.getProfile(
	             config.regionID,         
	             config.accessKey,     
	             config.accessSecret);
	    IAcsClient client = new DefaultAcsClient(profile);
	    //get master ID
	    String mid = null;
	    DescribeDBInstanceHAConfigRequest request = new DescribeDBInstanceHAConfigRequest();
	    request.setActionName("DescribeDBInstanceHAConfig");
	    request.setDBInstanceId(iid);	    
	    DescribeDBInstanceHAConfigResponse response;
        try {
            response = client.getAcsResponse(request);
            for (NodeInfo nd:response.getHostInstanceInfos()) {
            	if (nd.getNodeType().equals("Master")) {
            		mid = nd.getNodeId();
            		logger.log(INFO,"Master ID:" + mid);
            	}            	
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
	    DescribeBinlogFilesRequest request1 = new DescribeBinlogFilesRequest();
	    //request.setPageSize(10);
	    request1.setActionName("DescribeBinlogFiles");
	    request1.setDBInstanceId(iid);
	    request1.setStartTime(startDate);
	    request1.setEndTime(endDate);
	    
	    DescribeBinlogFilesResponse response1;
        try {
           response1 = client.getAcsResponse(request1);
           for (BinLogFile bl:response1.getItems()) {
        	   String dl = bl.getDownloadLink();
        	   for (int i = 0;i< downloadList.size();i++) {
        		   String fn = downloadList.get(i);
        		   if (dl.contains(fn) && dl.contains(mid)) {
        			   logger.log(INFO,dl + " " + fn);
        			   downloadUntarFile(dl, fn);
        		   }
        	   }
               //System.out.println(bl.getDownloadLink());
           }
       } catch (Exception e) {
           e.printStackTrace();
       }
		
	}
	
	private void downloadUntarFile(String dl, String filename) {
		logger.log(INFO,"Start to download/untar file");
		//int bytesum = 0;
		int byteread = 0;
		try {			
			URL url = new URL(dl);
			URLConnection conn = url.openConnection();
			InputStream inStream = conn.getInputStream();
			
			String outDir = config.filePath;
			//FileOutputStream fs = new FileOutputStream("src/main/resources/" + filename);
			
			TarArchiveInputStream in = new TarArchiveInputStream(inStream);
		    TarArchiveEntry entry = in.getNextTarEntry();
		    while (entry != null) {
		        if (entry.isDirectory()) {
		            entry = in.getNextTarEntry();
		            continue;
		        }
		        File curfile = new File(outDir, entry.getName());
		        File parent = curfile.getParentFile();
		        if (!parent.exists()) {
		            parent.mkdirs();
		        }
		        OutputStream out = new FileOutputStream(curfile);
		        IOUtils.copy(in, out);
		        out.close();
		        entry = in.getNextTarEntry();
		    }
		    in.close();
		    
			logger.log(INFO,"Downloaded/Untarred binlog" + dl + filename);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	protected Position getInitialPosition() throws Exception {
		/* first method:  do we have a stored position for this server? */
		Position initial = this.context.getInitialPosition();

		if (initial == null) {
			/* third method: capture the current master position. */
			if ( initial == null ) {
				try ( Connection c = context.getReplicationConnection() ) {
					initial = Position.capture(c, config.gtidMode);
				}
			}

			/* if the initial position didn't come from the store, store it */
			context.getPositionStore().set(initial);
		}
		return initial;
	}
	
	protected ArrayList<String> getDownLoadList(String posFile) throws Exception {
		logger.log(INFO,"Current binlog:" + posFile);
		ArrayList<String> l = null;
		ArrayList<String> dl = new ArrayList<String>();
		
		try (Connection c = context.getReplicationConnection()) {
			// return online logs: show master logs
			l = BinlogPosition.showLogs(c);

			for (int i = 0; i < l.size(); i++) {
				String f = l.get(i);
				logger.log(INFO,"online binlog:" + i + " " + f);
			}
			
			//String cf = initial.getBinlogPosition().getFile();//current binlog file: mysql-bin.000xxx
			String[] cfa = posFile.split("\\.");
			int cfi = Integer.valueOf(cfa[1]);
			String tf = l.get(0);//mysql-bin.000xxx
			cfa = tf.split("\\.");
			int tfi = Integer.valueOf(cfa[1]);
			
			if (cfi < tfi) {
				logger.log(INFO,"Start to retrieve target download list");				
			} else {
				throw new Exception("Target binlog is online, nothing to download");
			}
			
			for (int i = cfi;i < tfi; i++) {
				String index = String.format("%06d",i);
				logger.log(INFO,"mysql-bin." + index);
				//String.format("%04d",i);
				dl.add("mysql-bin." + index);
			}			
			return dl;
		}
	}
	
	private void startInner() throws Exception {
		try ( Connection connection = this.context.getReplicationConnection();
		      Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);
			SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);

			try ( Connection schemaConnection = this.context.getMaxwellConnection() ) {
				SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
			}
		}

		AbstractProducer producer = this.context.getProducer();
		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		Position initPosition = getInitialPosition();
		
		if (this.config.download) {
			downLoadBinlog(getDownLoadList(initPosition.getBinlogPosition().getFile()));
		} else {
			logger.log(INFO, "Skipping download binlog");
		}
		ArrayList<String> fl = getFileList();

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initPosition);
		mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.		

		this.replicator = new BinlogConnectorFileReader(mysqlSchemaStore,
				producer, bootstrapper, this.context, initPosition, fl);
        
		replicator.setFilter(context.getFilter());

		context.setReplicator(replicator);
		//this.context.start();
		//this.onReplicatorStart();
		replicator.runLoop();
	}

	public static void main(String[] args) {		
		try {
			logger.log(INFO,"Maxwell Reader Start!!");
			MaxwellConfig config = new MaxwellConfig(args);
			MaxwellFileReader maxwell = new MaxwellFileReader(config);
			maxwell.startInner();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.log(INFO,"Exception in main",e);
			e.printStackTrace();
		}

	}

}
