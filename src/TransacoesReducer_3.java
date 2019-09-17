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
            // Se já estiver no hashmap
            if (hashMap.get(v.getMercadoria()) != null) {
                // Incrementa o valor armazenado
                Integer novoN = hashMap.get(v.getMercadoria()) + v.getN();
                hashMap.put(v.getMercadoria(), novoN);
            } else {
                // Se for novo, apenas é colocado no hashmap
                hashMap.put(v.getMercadoria(), v.getN());
            }
        }

        Integer maiorN = 0;
        // Variável de saída
        String saida = "";

        // Para cada valor na HashMap
        for (HashMap.Entry<String, Integer> pair : hashMap.entrySet()) {
            // Se o valor for maior que o armazanado
            if (pair.getValue() > maiorN) {
                // Os valores são sobreescritos
                maiorN = pair.getValue();
                saida = "\n" + pair.getKey() + " " + pair.getValue();
            } else if (pair.getValue().equals(maiorN)) {
                // Se forem iguais, os resultados são concatenados
                saida += "\n" + pair.getKey() + " " + pair.getValue();
            }
        }

        context.write(key, new Text(saida));

    }
}


