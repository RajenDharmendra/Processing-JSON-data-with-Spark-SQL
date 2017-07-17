# Processing-JSON-data-with-Spark-SQL
Processing JSON data with Spark SQL



Spark SQL provides built-in support for variety of data formats, including JSON. Each new release of Spark contains enhancements that make use of DataFrames API with JSON data more convenient. Same time, there are a number of tricky aspects that might lead to unexpected results. In this post I’ll show how to use Spark SQL to deal with JSON.

<p>JSON is very simple, human-readable and easy to use format.
But its simplicity can lead to problems, since it&rsquo;s schema-less.
Especially when you have to deal with unreliable third-party data sources,
such services may return crazy JSON responses containing integer numbers as strings,
or encode nulls different ways like <code>null</code>, <code>""</code> or even <code>"null"</code>.</p>

<h2>Loading data</h2>

<p>You can read and parse JSON to DataFrame directly from file:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="k">val</span> <span class="n">df</span> <span class="k">=</span> <span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">json</span><span class="o">(</span><span class="s">&quot;s3a://some-bucket/some-file.json&quot;</span><span class="o">)</span>
</span></code></pre></td></tr></table></div></figure>


<p>Please note Spark expects each line to be a separate JSON object,
so it will fail if you&rsquo;ll try to load a pretty formatted JSON file.</p>

<p>Also you read JSON data from <code>RDD[String]</code> object like:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="c1">// construct RDD[Sting]</span>
</span><span class='line'><span class="k">val</span> <span class="n">events</span> <span class="k">=</span> <span class="n">sc</span><span class="o">.</span><span class="n">parallelize</span><span class="o">(</span>
</span><span class='line'>  <span class="s">&quot;&quot;&quot;{&quot;action&quot;:&quot;create&quot;,&quot;timestamp&quot;:&quot;2016-01-07T00:01:17Z&quot;}&quot;&quot;&quot;</span> <span class="o">::</span> <span class="nc">Nil</span><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="c1">// read it</span>
</span><span class='line'><span class="k">val</span> <span class="n">df</span> <span class="k">=</span> <span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">json</span><span class="o">(</span><span class="n">events</span><span class="o">)</span>
</span></code></pre></td></tr></table></div></figure>


<p>The latter option is also useful for reading JSON messages
from Kafka with Spark Streaming.</p>

<p>If you are just playing around with DataFrames
you can use <code>show</code> method to print DataFrame to console.</p>

<pre><code>scala&gt; df.show
+------+--------------------+
|action|           timestamp|
+------+--------------------+
|create|2016-01-07T00:01:17Z|
+------+--------------------+
</code></pre>

<h2>Schema inference and explicit definition</h2>

<p>Simply running <code>sqlContext.read.json(events)</code> will not load data,
since DataFrames are evaluated lazily.
But it will trigger schema inference,
spark will go over RDD to determine schema that fits the data.</p>

<p>In the shell you can print schema using <code>printSchema</code> method:</p>

<pre><code>scala&gt; df.printSchema
root
 |-- action: string (nullable = true)
 |-- timestamp: string (nullable = true)
</code></pre>

<p>As you saw in the last example Spark inferred type of both columns as strings.</p>

<p>It is possible to provide schema explicitly to avoid that extra scan:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
<span class='line-number'>7</span>
<span class='line-number'>8</span>
<span class='line-number'>9</span>
<span class='line-number'>10</span>
<span class='line-number'>11</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="k">val</span> <span class="n">schema</span> <span class="k">=</span> <span class="o">(</span><span class="k">new</span> <span class="nc">StructType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;action&quot;</span><span class="o">,</span> <span class="nc">StringType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;timestamp&quot;</span><span class="o">,</span> <span class="nc">TimestampType</span><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="k">val</span> <span class="n">df</span> <span class="k">=</span> <span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">schema</span><span class="o">(</span><span class="n">schema</span><span class="o">).</span><span class="n">json</span><span class="o">(</span><span class="n">events</span><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="n">df</span><span class="o">.</span><span class="n">show</span>
</span><span class='line'>
</span><span class='line'><span class="c1">// +------+--------------------+</span>
</span><span class='line'><span class="c1">// |action|           timestamp|</span>
</span><span class='line'><span class="c1">// +------+--------------------+</span>
</span><span class='line'><span class="c1">// |create|2016-01-07 01:01:...|</span>
</span><span class='line'><span class="c1">// +------+--------------------+</span>
</span></code></pre></td></tr></table></div></figure>


<p>As you might have noticed type of <code>timestamp</code> column is explicitly forced to be a <code>TimestampType</code>.
It&rsquo;s important to understand that this type coercion is performed in JSON parser,
and it has nothing to do with DataFrame&rsquo;s type casting functionality.
Type coercions implemented in parser are somewhat limited and in some cases unobvious.
Following example demonstrates it:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
<span class='line-number'>7</span>
<span class='line-number'>8</span>
<span class='line-number'>9</span>
<span class='line-number'>10</span>
<span class='line-number'>11</span>
<span class='line-number'>12</span>
<span class='line-number'>13</span>
<span class='line-number'>14</span>
<span class='line-number'>15</span>
<span class='line-number'>16</span>
<span class='line-number'>17</span>
<span class='line-number'>18</span>
<span class='line-number'>19</span>
<span class='line-number'>20</span>
<span class='line-number'>21</span>
<span class='line-number'>22</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="k">val</span> <span class="n">events</span> <span class="k">=</span> <span class="n">sc</span><span class="o">.</span><span class="n">parallelize</span><span class="o">(</span>
</span><span class='line'>  <span class="s">&quot;&quot;&quot;{&quot;action&quot;:&quot;create&quot;,&quot;timestamp&quot;:1452121277}&quot;&quot;&quot;</span> <span class="o">::</span>
</span><span class='line'>  <span class="s">&quot;&quot;&quot;{&quot;action&quot;:&quot;create&quot;,&quot;timestamp&quot;:&quot;1452121277&quot;}&quot;&quot;&quot;</span> <span class="o">::</span>
</span><span class='line'>  <span class="s">&quot;&quot;&quot;{&quot;action&quot;:&quot;create&quot;,&quot;timestamp&quot;:&quot;&quot;}&quot;&quot;&quot;</span> <span class="o">::</span>
</span><span class='line'>  <span class="s">&quot;&quot;&quot;{&quot;action&quot;:&quot;create&quot;,&quot;timestamp&quot;:null}&quot;&quot;&quot;</span> <span class="o">::</span>
</span><span class='line'>  <span class="s">&quot;&quot;&quot;{&quot;action&quot;:&quot;create&quot;,&quot;timestamp&quot;:&quot;null&quot;}&quot;&quot;&quot;</span> <span class="o">::</span>
</span><span class='line'>  <span class="nc">Nil</span>
</span><span class='line'><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="k">val</span> <span class="n">schema</span> <span class="k">=</span> <span class="o">(</span><span class="k">new</span> <span class="nc">StructType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;action&quot;</span><span class="o">,</span> <span class="nc">StringType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;timestamp&quot;</span><span class="o">,</span> <span class="nc">LongType</span><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">schema</span><span class="o">(</span><span class="n">schema</span><span class="o">).</span><span class="n">json</span><span class="o">(</span><span class="n">events</span><span class="o">).</span><span class="n">show</span>
</span><span class='line'>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span><span class='line'><span class="c1">// |action| timestamp|</span>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span><span class='line'><span class="c1">// |create|1452121277|</span>
</span><span class='line'><span class="c1">// |  null|      null|</span>
</span><span class='line'><span class="c1">// |create|      null|</span>
</span><span class='line'><span class="c1">// |create|      null|</span>
</span><span class='line'><span class="c1">// |  null|      null|</span>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span></code></pre></td></tr></table></div></figure>


<p>Frankly that is not a result that one can expect.
Look at 2nd row in the result set, as you may see, there is no conversion from string to integer.
But here is one more big problem, if you try to set type for which parser doesn&rsquo;t has conversion,
it won&rsquo;t simply discard value and set that field to <code>null</code>,
instead it will consider entire row as incorrect, and set all fields to <code>null</code>s.
The good news is that you can read all values as strings.</p>

<h2>Type casting</h2>

<p>If you can&rsquo;t be sure in a quality of you data,
the best option is to explicitly provide schema forcing <code>StringType</code> for all untrusted fields
to avoid extra RDD scan, and then cast those columns to desired type:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
<span class='line-number'>7</span>
<span class='line-number'>8</span>
<span class='line-number'>9</span>
<span class='line-number'>10</span>
<span class='line-number'>11</span>
<span class='line-number'>12</span>
<span class='line-number'>13</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="k">val</span> <span class="n">schema</span> <span class="k">=</span> <span class="o">(</span><span class="k">new</span> <span class="nc">StructType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;action&quot;</span><span class="o">,</span> <span class="nc">StringType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;timestamp&quot;</span><span class="o">,</span> <span class="nc">StringType</span><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">schema</span><span class="o">(</span><span class="n">schema</span><span class="o">).</span><span class="n">json</span><span class="o">(</span><span class="n">events</span><span class="o">).</span><span class="n">select</span><span class="o">(</span><span class="n">$</span><span class="s">&quot;action&quot;</span><span class="o">,</span> <span class="n">$</span><span class="s">&quot;timestamp&quot;</span><span class="o">.</span><span class="n">cast</span><span class="o">(</span><span class="nc">LongType</span><span class="o">)).</span><span class="n">show</span>
</span><span class='line'>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span><span class='line'><span class="c1">// |action| timestamp|</span>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span><span class='line'><span class="c1">// |create|1452121277|</span>
</span><span class='line'><span class="c1">// |create|1452121277|</span>
</span><span class='line'><span class="c1">// |create|      null|</span>
</span><span class='line'><span class="c1">// |create|      null|</span>
</span><span class='line'><span class="c1">// |create|      null|</span>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span></code></pre></td></tr></table></div></figure>


<p>Now that&rsquo;s more like a sane result.</p>

<p>Spark&rsquo;s catalyst optimizer has a very powerful type casting functionality,
let&rsquo;s see how we can parse UNIX timestamps from the previous example:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
<span class='line-number'>7</span>
<span class='line-number'>8</span>
<span class='line-number'>9</span>
<span class='line-number'>10</span>
<span class='line-number'>11</span>
<span class='line-number'>12</span>
<span class='line-number'>13</span>
<span class='line-number'>14</span>
<span class='line-number'>15</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="k">val</span> <span class="n">schema</span> <span class="k">=</span> <span class="o">(</span><span class="k">new</span> <span class="nc">StructType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;action&quot;</span><span class="o">,</span> <span class="nc">StringType</span><span class="o">).</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;timestamp&quot;</span><span class="o">,</span> <span class="nc">StringType</span><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">schema</span><span class="o">(</span><span class="n">schema</span><span class="o">).</span><span class="n">json</span><span class="o">(</span><span class="n">events</span><span class="o">)</span>
</span><span class='line'>  <span class="o">.</span><span class="n">select</span><span class="o">(</span><span class="n">$</span><span class="s">&quot;action&quot;</span><span class="o">,</span> <span class="n">$</span><span class="s">&quot;timestamp&quot;</span><span class="o">.</span><span class="n">cast</span><span class="o">(</span><span class="nc">LongType</span><span class="o">).</span><span class="n">cast</span><span class="o">(</span><span class="nc">TimestampType</span><span class="o">))</span>
</span><span class='line'>  <span class="o">.</span><span class="n">show</span>
</span><span class='line'>
</span><span class='line'><span class="c1">// +------+--------------------+</span>
</span><span class='line'><span class="c1">// |action|           timestamp|</span>
</span><span class='line'><span class="c1">// +------+--------------------+</span>
</span><span class='line'><span class="c1">// |create|2016-01-07 00:01:...|</span>
</span><span class='line'><span class="c1">// |create|2016-01-07 00:01:...|</span>
</span><span class='line'><span class="c1">// |create|                null|</span>
</span><span class='line'><span class="c1">// |create|                null|</span>
</span><span class='line'><span class="c1">// |create|                null|</span>
</span><span class='line'><span class="c1">// +------+--------------------+</span>
</span></code></pre></td></tr></table></div></figure>


<p>Spark allows to parse integer timestamps as a timestamp type,
but right now (as of spark 1.6) there exists a difference in behavior:
parser treats integer value as a number of milliseconds,
but catalysts cast behavior is treat as a number of seconds.
This behavior is about to change in Spark&nbsp;2.0
(see <a href="https://issues.apache.org/jira/browse/SPARK-12744">SPARK-12744</a>).</p>

<h2>Handling nested objects</h2>

<p>Often in API responses useful data might be wrapped in a several layers of nested objects:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
<span class='line-number'>7</span>
<span class='line-number'>8</span>
</pre></td><td class='code'><pre><code class='json'><span class='line'><span class="p">{</span>
</span><span class='line'>  <span class="nt">&quot;payload&quot;</span><span class="p">:</span> <span class="p">{</span>
</span><span class='line'>    <span class="nt">&quot;event&quot;</span><span class="p">:</span> <span class="p">{</span>
</span><span class='line'>      <span class="nt">&quot;action&quot;</span><span class="p">:</span> <span class="s2">&quot;create&quot;</span><span class="p">,</span>
</span><span class='line'>      <span class="nt">&quot;timestamp&quot;</span><span class="p">:</span> <span class="mi">1452121277</span>
</span><span class='line'>    <span class="p">}</span>
</span><span class='line'>  <span class="p">}</span>
</span><span class='line'><span class="p">}</span>
</span></code></pre></td></tr></table></div></figure>


<p>Star (<code>*</code>) expansion makes it easier to unnest with such objects, for example:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
<span class='line-number'>7</span>
<span class='line-number'>8</span>
<span class='line-number'>9</span>
<span class='line-number'>10</span>
<span class='line-number'>11</span>
<span class='line-number'>12</span>
<span class='line-number'>13</span>
<span class='line-number'>14</span>
<span class='line-number'>15</span>
<span class='line-number'>16</span>
<span class='line-number'>17</span>
<span class='line-number'>18</span>
<span class='line-number'>19</span>
<span class='line-number'>20</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="k">val</span> <span class="n">vals</span> <span class="k">=</span> <span class="n">sc</span><span class="o">.</span><span class="n">parallelize</span><span class="o">(</span>
</span><span class='line'>  <span class="s">&quot;&quot;&quot;{&quot;payload&quot;:{&quot;event&quot;:{&quot;action&quot;:&quot;create&quot;,&quot;timestamp&quot;:1452121277}}}&quot;&quot;&quot;</span> <span class="o">::</span>
</span><span class='line'>  <span class="nc">Nil</span>
</span><span class='line'><span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="k">val</span> <span class="n">schema</span> <span class="k">=</span> <span class="o">(</span><span class="k">new</span> <span class="nc">StructType</span><span class="o">)</span>
</span><span class='line'>  <span class="o">.</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;payload&quot;</span><span class="o">,</span> <span class="o">(</span><span class="k">new</span> <span class="nc">StructType</span><span class="o">)</span>
</span><span class='line'>    <span class="o">.</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;event&quot;</span><span class="o">,</span> <span class="o">(</span><span class="k">new</span> <span class="nc">StructType</span><span class="o">)</span>
</span><span class='line'>      <span class="o">.</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;action&quot;</span><span class="o">,</span> <span class="nc">StringType</span><span class="o">)</span>
</span><span class='line'>      <span class="o">.</span><span class="n">add</span><span class="o">(</span><span class="s">&quot;timestamp&quot;</span><span class="o">,</span> <span class="nc">LongType</span><span class="o">)</span>
</span><span class='line'>    <span class="o">)</span>
</span><span class='line'>  <span class="o">)</span>
</span><span class='line'>
</span><span class='line'><span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">schema</span><span class="o">(</span><span class="n">schema</span><span class="o">).</span><span class="n">json</span><span class="o">(</span><span class="n">vals</span><span class="o">).</span><span class="n">select</span><span class="o">(</span><span class="n">$</span><span class="s">&quot;payload.event.*&quot;</span><span class="o">).</span><span class="n">show</span>
</span><span class='line'>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span><span class='line'><span class="c1">// |action| timestamp|</span>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span><span class='line'><span class="c1">// |create|1452121277|</span>
</span><span class='line'><span class="c1">// +------+----------+</span>
</span></code></pre></td></tr></table></div></figure>


<p>If you need more control over column names,
you can always use <code>as</code> method to rename columns, e.g.:</p>

<figure class='code'><figcaption><span></span></figcaption><div class="highlight"><table><tr><td class="gutter"><pre class="line-numbers"><span class='line-number'>1</span>
<span class='line-number'>2</span>
<span class='line-number'>3</span>
<span class='line-number'>4</span>
<span class='line-number'>5</span>
<span class='line-number'>6</span>
<span class='line-number'>7</span>
<span class='line-number'>8</span>
<span class='line-number'>9</span>
<span class='line-number'>10</span>
<span class='line-number'>11</span>
</pre></td><td class='code'><pre><code class='scala'><span class='line'><span class="n">sqlContext</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">schema</span><span class="o">(</span><span class="n">schema</span><span class="o">).</span><span class="n">json</span><span class="o">(</span><span class="n">vals</span><span class="o">)</span>
</span><span class='line'>  <span class="o">.</span><span class="n">select</span><span class="o">(</span>
</span><span class='line'>    <span class="n">$</span><span class="s">&quot;payload.event.action&quot;</span><span class="o">.</span><span class="n">as</span><span class="o">(</span><span class="s">&quot;event_action&quot;</span><span class="o">),</span>
</span><span class='line'>    <span class="n">$</span><span class="s">&quot;payload.event.timestamp&quot;</span><span class="o">.</span><span class="n">as</span><span class="o">(</span><span class="s">&quot;event_timestamp&quot;</span><span class="o">)</span>
</span><span class='line'>  <span class="o">).</span><span class="n">show</span>
</span><span class='line'>
</span><span class='line'><span class="c1">// +------------+---------------+</span>
</span><span class='line'><span class="c1">// |event_action|event_timestamp|</span>
</span><span class='line'><span class="c1">// +------------+---------------+</span>
</span><span class='line'><span class="c1">// |      create|     1452121277|</span>
</span><span class='line'><span class="c1">// +------------+---------------+</span>
</span></code></pre></td></tr></table></div></figure>
