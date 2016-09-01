package com.zendesk.maxwell.distributed;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellLogging;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by springloops on 2016. 8. 29..
 */
public class ActiveStandbyMaxwell implements Runnable {
    static final Logger LOGGER = LoggerFactory.getLogger(ActiveStandbyMaxwell.class);

    private final HAConfig haConfig;
    private final MaxwellContext context;

    private HelixManager participantManager;
    private HelixManager controllerManager;
    private ActiveMaxwellLockFactory activeMaxwellLockFactory;

    public ActiveStandbyMaxwell(HAMaxwellConfig haConf) throws SQLException {
        this.haConfig = haConf;

        if (this.haConfig.getMaxwellConfig().log_level != null)
            MaxwellLogging.setLevel(this.haConfig.getMaxwellConfig().log_level);

        this.context = new MaxwellContext(this.haConfig.getMaxwellConfig());
    }

    public void run() {
        try {
            start();
        } catch (Exception e) {
            LOGGER.error("active-standby maxwell encountered an exception", e);
        }
    }

    public void terminate() {

        activeMaxwellLockFactory.getActiveMaxwellLock().getMaxwell().terminate();

        if (participantManager != null) {
            participantManager.disconnect();
        }

        if (controllerManager != null) {
            controllerManager.disconnect();
        }
    }

    private void start() throws Exception {
        configureInstance();

        try {
            addParticipant();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (haConfig.getStartController()) {
            startContoller();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                terminate();
            }
        });

        Thread.currentThread().join();
    }

    private void configureInstance() {
        LOGGER.info("Creating Maxwell Cluster into Zookeeper");

        String maxwellActiveStandbyResourceName = "ActiveStandbyResource";

        ZKHelixAdmin helixAdmin = new ZKHelixAdmin(haConfig.getZkAddress());

        helixAdmin.addCluster(haConfig.getClusterName(), false);

        if (helixAdmin.getStateModelDefs(haConfig.getClusterName()).isEmpty()) {
            helixAdmin.addStateModelDef(haConfig.getClusterName(), "OnlineOffline", new StateModelDefinition(
                    StateModelConfigGenerator.generateConfigForOnlineOffline()));
            helixAdmin.addResource(haConfig.getClusterName(), maxwellActiveStandbyResourceName, 1, "OnlineOffline",
                    IdealState.RebalanceMode.FULL_AUTO.toString());
            helixAdmin.rebalance(haConfig.getClusterName(), maxwellActiveStandbyResourceName, 1);
        }

        List<String> instanceInCluster = helixAdmin.getInstancesInCluster(haConfig.getClusterName());
        if (instanceInCluster == null || !instanceInCluster.contains(haConfig.getInstanceName())) {
            InstanceConfig instanceConfig = new InstanceConfig(haConfig.getInstanceName());
            instanceConfig.setHostName(haConfig.getHostName());
            instanceConfig.setPort(haConfig.getClusterPort());
            helixAdmin.addInstance(haConfig.getClusterName(), instanceConfig);
        }

        activeMaxwellLockFactory = new ActiveMaxwellLockFactory(this.context);

        LOGGER.info("Started Maxwell Active-Standby Mode");
    }

    private void addParticipant() throws Exception {
        LOGGER.info("Adding participant into Maxwell Cluster");
        participantManager = HelixManagerFactory.getZKHelixManager(haConfig.getClusterName(), haConfig.getInstanceName(), InstanceType.PARTICIPANT, haConfig.getZkAddress());
        participantManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline", activeMaxwellLockFactory);
        participantManager.connect();
    }

    private void startContoller() {
        LOGGER.info("start helix controller");
        controllerManager = HelixControllerMain.startHelixController(haConfig.getZkAddress(), haConfig.getClusterName(), haConfig.getClusterName()+"_Controller", HelixControllerMain.STANDALONE);
    }

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                StaticShutdownCallbackRegistry.invoke();
            }
        });

        try {

            final HAMaxwellConfig haConfig = new HAMaxwellConfig(args);
            if (haConfig.getMaxwellConfig().log_level != null)
                MaxwellLogging.setLevel(this.haConfig.getMaxwellConfig().log_level);

            new ActiveStandbyMaxwell(haConfig).start();
        } catch ( Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
