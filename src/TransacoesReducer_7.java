
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacoesReducer_7 extends Reducer<Text, TransacoesWritable, Text, Text> {

    public void reduce(Text key,
                       Iterable<TransacoesWritable> values,
                       Context context) throws IOException, InterruptedException {

        float maiorValor = 0;
        String maiorMercadoria = "error";
        String saidaTemp = " ";

        // Para cada valor
        for (TransacoesWritable v : values
        ) {
            if (v.getPeso() == 0) continue;
            float temp = ((float) v.getValor() / v.getPeso());
            if (maiorValor < temp) {
                maiorValor = temp;
                maiorMercadoria = v.getMercadoria();
                saidaTemp = maiorMercadoria + " " + maiorValor;
            }
        }

        // Variável de saída
        Text saida = new Text(saidaTemp);

        context.write(key, saida);

    }

}
