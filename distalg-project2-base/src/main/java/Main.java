import pt.unl.fct.di.novasys.babel.core.Babel;
import utils.InterfaceToIp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.IncorrectAgreement;
import protocols.app.HashApp;
import protocols.statemachine.StateMachine;

import java.util.Properties;


public class Main {

    //Sets the log4j (logging library) configuration file
    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    //Creates the logger object
    private static final Logger logger = LogManager.getLogger(Main.class);

    //Default babel configuration file (can be overridden by the "-config" launch argument)
    private static final String DEFAULT_CONF = "babel_config.properties";

    public static void main(String[] args) throws Exception {

        //Get the (singleton) babel instance
        Babel babel = Babel.getInstance();

        //Loads properties from the configuration file, and merges them with
        // properties passed in the launch arguments
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        //If you pass an interface name in the properties (either file or arguments), this wil get the
        // IP of that interface and create a property "address=ip" to be used later by the channels.
        if(props.containsKey("babel.interface"))
        	InterfaceToIp.addInterfaceIp(props);
        else if(!props.containsKey("babel.address")) {
        	System.err.println("Cannot start process without either babel.interface or babel.address being defined.");
        	System.exit(1);
        }

        // Application
        HashApp hashApp = new HashApp(props);
        // StateMachine Protocol
        StateMachine sm = new StateMachine(props);
        // Agreement Protocol
        IncorrectAgreement agreement = new IncorrectAgreement(props);

        //Register applications in babel
        babel.registerProtocol(hashApp);
        babel.registerProtocol(sm);
        babel.registerProtocol(agreement);

        //Init the protocols. This should be done after creating all protocols,
        // since there can be inter-protocol communications in this step.
        hashApp.init(props);
        sm.init(props);
        agreement.init(props);

        //Start babel and protocol threads
        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Goodbye")));

    }

}
