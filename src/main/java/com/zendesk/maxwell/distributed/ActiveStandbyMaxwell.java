package com.zendesk.maxwell.distributed;

import com.zendesk.maxwell.Maxwell;
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

import java.util.List;

/**
 * Created by min on 2016. 8. 29..
 */
public class ActiveStandbyMaxwell implements Maxwell {
    static final Logger LOGGER = LoggerFactory.getLogger(ActiveStandbyMaxwell.class);

    private MaxwellConfig config;
    private MaxwellContext context;

    private final String _zkAddress;
    private final String _clusterName;
    private final String _instanceName;
    private final String _hostName;
    private final String _port;

    private HelixManager participantManager = null;
    private HelixManager controllerManager = null;

    public ActiveStandbyMaxwell(MaxwellConfig conf) {
        this.config = conf;

        _zkAddress = config.zkAddress;
        _clusterName = config.clusterName;
        _instanceName = config.instanceName;
        _hostName = config.hostName;
        _port = config.clusterPort;


        if (this.config.log_level != null)
            MaxwellLogging.setLevel(this.config.log_level);

        this.context = new MaxwellContext(this.config);
    }

    @Override
    public void run() throws Exception {
        configureInstance();

        addParticipant();

        if (config.startController) {
            startContoller();
        }
    }

    private void configureInstance() {
        LOGGER.info("Creating Maxwell Cluster into Zookeeper");

        String maxwellActiveStandbyResourceName = "ActiveStandbyResource";

        ZKHelixAdmin helixAdmin = new ZKHelixAdmin(_zkAddress);

        helixAdmin.addCluster(_clusterName, false);

        if (helixAdmin.getStateModelDefs(_clusterName).isEmpty()) {
            helixAdmin.addStateModelDef(_clusterName, "OnlineOffline", new StateModelDefinition(
                    StateModelConfigGenerator.generateConfigForOnlineOffline()));
            helixAdmin.addResource(_clusterName, maxwellActiveStandbyResourceName, 1, "OnlineOffline",
                    IdealState.RebalanceMode.FULL_AUTO.toString());
            helixAdmin.rebalance(_clusterName, maxwellActiveStandbyResourceName, 1);
        }

        List<String> instanceInCluster = helixAdmin.getInstancesInCluster(_clusterName);
        if (instanceInCluster == null || !instanceInCluster.contains(_instanceName)) {
            InstanceConfig instanceConfig = new InstanceConfig(_instanceName);
            instanceConfig.setHostName(_hostName);
            instanceConfig.setPort(_port);
            helixAdmin.addInstance(_clusterName, instanceConfig);
        }

        LOGGER.info("Started Maxwell Active-Standby Mode");
    }

    private void addParticipant() throws Exception {
        LOGGER.info("Adding participant into Maxwell Cluster");
        participantManager = HelixManagerFactory.getZKHelixManager(_clusterName, _instanceName, InstanceType.PARTICIPANT, _zkAddress);
        participantManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline", new ActiveMaxwellLockFactory(this.context));
        participantManager.connect();
    }

    private void startContoller() {
        LOGGER.info("start helix controller");
        controllerManager = HelixControllerMain.startHelixController(_zkAddress, _clusterName, _clusterName+"_Controller", HelixControllerMain.STANDALONE);
    }

    @Override
    public void stop() {
        if (participantManager != null) {
            participantManager.disconnect();
        }

        if (controllerManager != null) {
            controllerManager.disconnect();
        }
    }
}
