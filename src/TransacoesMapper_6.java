import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransacoesMapper_6 extends Mapper<LongWritable, Text, Text, TransacoesWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        // Cada linha
        String conteudo = value.toString();
        // Separar por colunas
        String[] valores = conteudo.split(";");

        // Ignora o header e vazio
        if (valores[6].equals("weight_kg") || valores[6].equals("")) return;

        // A chave é mercadoria + ano para agrupar por mercadoria primeiro, Ex:
        // Strawberries 2014: 28664.00
        // Strawberries 2016: 32644.00
        Text chave = new Text(valores[3] + " " + valores[1]);

        // O valor é o peso + n
        TransacoesWritable valor = new TransacoesWritable();
        valor.setN(1);
        valor.setPeso(Long.parseLong(valores[6]));
        valor.setValor(Long.parseLong(valores[5]));

        // Apenas se o país for 'Brazil'
        if (valores[0].equals("Brazil")) {
            // Passando isso pro reduce
            context.write(chave, valor);
        }

    }
}