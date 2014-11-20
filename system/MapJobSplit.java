class MapJobSplit {
        InputSplit split;
        JobState state;
        int workerID;
        String[] intermediateFilenames;

        public MapJobSplit(InputSplit split) {
            this.split = split;
            state = JobState.IDLE;
            workerID = -1;
            intermediateFilenames = null;
        }

        public String[] getIntermediateFilenames() {
            return intermediateFilenames;
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