package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class StealLeaderRequest extends ProtoRequest {

    public static final short REQUEST_ID = 105;

    public StealLeaderRequest() {
        super(REQUEST_ID);
    }

    // Don't need host, because the replica is going to call for himself

    @Override
    public String toString() {
        return "StealLeaderRequest{}";
    }
}