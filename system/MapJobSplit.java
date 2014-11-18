class MapJobSplit {
        InputSplit split;
        JobState state;
        int workerId;
        String[] intermediateFilenames;

        public MapJobSplit(InputSplit split) {
            this.split = split;
            state = JobState.IDLE;
            workerId = -1;
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

        public JobState getState() {
            return state;
        }

        public void setState(JobState s) {
            state = s;
        }

        public int getWorkerId() {
            return workerId;
        }

        public void setWorkerId(int id) {
            workerId = id;
        }

        public InputSplit getInputSplit() {
            return split;
        }
    }