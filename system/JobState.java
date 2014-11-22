package system;

import java.io.Serializable;

public enum JobState implements Serializable {
    IDLE, 
    IN_PROGRESS, 
    COMPLETED
}