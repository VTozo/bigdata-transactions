import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TransacoesReducer_3 extends Reducer<Text, TransacoesWritable, Text, Text> {

    public void reduce(Text key,
                       Iterable<TransacoesWritable> values,
                       Context context) throws IOException, InterruptedException {

        // Cria um HashMap para armazenar os valores
        HashMap<String, Integer> hashMap = new HashMap<>();

        // Para cada valor
        for (TransacoesWritable v : values
        ) {
            if (hashMap.get(v.getMercadoria()) != null) {
                Integer novoN = hashMap.get(v.getMercadoria()) + v.getN();
                hashMap.put(v.getMercadoria(), novoN);
            } else {
                hashMap.put(v.getMercadoria(), v.getN());
            }
        }

        Integer maiorN = 0;
        // Variável de saída
        String saida = "";

        // Para cada valor na HashMap
        for (HashMap.Entry<String, Integer> pair : hashMap.entrySet()) {
            
            if (pair.getValue() > maiorN) {
                maiorN = pair.getValue();
                saida = "\n" + pair.getKey() + " " + pair.getValue();
            } else if (pair.getValue().equals(maiorN)) {
                saida += "\n" + pair.getKey() + " " + pair.getValue();
            }
        }

        context.write(key, new Text(saida));

    }
}


