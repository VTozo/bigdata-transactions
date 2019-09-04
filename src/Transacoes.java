import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;

import java.util.Scanner;

public class Transacoes {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in\\transactions.csv");

        // arquivo de saida
        Path output = new Path("output\\transactions-" + System.currentTimeMillis());

        // criacao do job e seu nome
        Job j = Job.getInstance(c, "transacoes");

        Scanner scanner = new Scanner(System.in);  // Criar objeto scanner para receber input do usuário

        System.out.println("Digite o número do exercício: ");

        String exercicio = scanner.nextLine();  // Ler input do usuário

        switch (exercicio) {
            case "1":
                // Registrar as classes
                j.setJarByClass(Transacoes.class);
                j.setMapperClass(TransacoesMapper_1.class);
                j.setCombinerClass(TransacoesReducer_1.class);
                j.setReducerClass(TransacoesReducer_1.class);

                // Definição dos tipos de saída
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);

                break;
            case "2":
                // Registrar as classes
                j.setJarByClass(Transacoes.class);
                j.setMapperClass(TransacoesMapper_2.class);
                j.setCombinerClass(TransacoesReducer_2.class);
                j.setReducerClass(TransacoesReducer_2.class);

                // Definição dos tipos de saída
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);
                break;
            case "3":
                // Registrar as classes
                j.setJarByClass(Transacoes.class);
                j.setMapperClass(TransacoesMapper_3.class);
                j.setCombinerClass(TransacoesReducer_3.class);
                j.setReducerClass(TransacoesReducer_3.class);

                // Definição dos tipos de saída
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);
                break;

        }

        // Passando para o job os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

}
