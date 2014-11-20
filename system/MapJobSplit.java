package system;

import lib.input.InputSplit;

class MapJobSplit {
        InputSplit split;
        JobState state;
        int workerID;

        // Size == NUM_REDUCERS
        // intermediateFilenames[i] == null if corresponding
        // partition has no map output
        String[] intermediateFilenames;

        public MapJobSplit(InputSplit split) {
            this.split = split;
            state = JobState.IDLE;
            workerID = -1;
            intermediateFilenames = null;
        }

        public String getIntermediateFilename(int i) {
            if (i < 0 || i >= intermediateFilenames.length)
                return null;
            return intermediateFilenames[i];
        }

        public void setIntermediateFilenames(String[] filenames) {
            int n = filenames.length;
            intermediateFilenames = new String[n];
            for (int i = 0; i < n; i++)
                intermediateFilenames[i] = filenames[i];
        }

        public JobState getJobState() {
            return state;
        }

        public void setJobState(JobState s) {
            state = s;
        }

        public int getWorkerID() {
            return workerID;
        }									

        public void setWorkerID(int id) {
            workerID = id;
        }

        public InputSplit getInputSplit() {
            return split;
        }
    }