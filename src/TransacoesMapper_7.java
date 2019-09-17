import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransacoesMapper_7 extends Mapper<LongWritable, Text, Text, TransacoesWritable>{

    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        // Cada linha
        String conteudo = value.toString();
        // Separar por colunas
        String[] valores = conteudo.split(";");

        // Ignora o header e vazio
        if (valores[5].equals("")) return;
        if (valores[6].equals("weight_kg") || valores[6].equals("")) return;
        if (valores[3].equals(" ")) return;

        // A chave é a mercadoria
        Text chave = new Text("Maior valor por peso");

        // O valor é o valor / peso
        TransacoesWritable valor = new TransacoesWritable();
        valor.setN(1);
        valor.setValor(Long.parseLong(valores[5]));
        valor.setPeso(Long.parseLong(valores[6]));
        valor.setMercadoria(valores[3]);


        // Passando isso pro reduce
        context.write(chave, valor);


    }
}
