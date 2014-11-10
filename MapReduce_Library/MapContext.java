import java.util.ArrayList;
public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    extends Context<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    class Pair<KEYIN, VALUEIN> {
        private KEYIN key;
        private VALUEIN value;

        public Pair(KEYIN key, VALUEIN value) {
            this.key = key;
            this.value = value;
        }

        public KEYIN getKey() {
            return key;
        }

        public VALUEIN getValue() {
            return value;
        }
    }

    private ArrayList<Pair<KEYIN, VALUEIN>> inList;
    private counter = 0;

    public Context(ArrayList<KEYIN> keys, ArrayList<VALUEIN> values) throws Exception {
        if (keys.size() != values.size())
            throw new Exception("Sizes of key list and value list do not match.");

        inList = new ArrayList<Pair<KEYIN, VALUEIN>>();
        for (int i = 0; i < keys.size(); i++)
            inList.add(new Pair(keys.get(i), values.get(i)));
        counter = -1;
    }

    public KEYIN getCurrentKey() {
        if (counter >= inList.size())
            return null;
        return inList.get(counter).getKey();
    }

    public VALUEIN getCurrentValue() {
        if (counter >= inList.size())
            return null;
        return inList.get(counter).getValue();
    }

    public boolean nextKeyValue() {
        if (counter >= inList.size())
            return false;
        return (++counter) < inList.size();
    }
}