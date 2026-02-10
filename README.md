<html lang="en"><head>
    <meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <title>Apache Spark End-to-End: Deep Dive into Shuffles, Spillage, and DAG Execution</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Canela:wght@300;400;700&amp;family=Inter:wght@300;400;500;600;700&amp;display=swap" rel="stylesheet"/>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"/>
    <style>
        :root {
            --primary: #2c1810;
            --secondary: #8b7355;
            --accent: #d4af37;
            --neutral: #f5f3f0;
            --base-100: #ffffff;
        }
        
        .font-canela { font-family: 'Canela', serif; }
        .font-inter { font-family: 'Inter', sans-serif; }
        
        body {
            background: linear-gradient(135deg, #f5f3f0 0%, #ffffff 100%);
            color: var(--primary);
            line-height: 1.7;
        }
        
        .toc-sidebar {
            position: fixed;
            top: 0;
            left: 0;
            width: 280px;
            height: 100vh;
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-right: 1px solid rgba(44, 24, 16, 0.1);
            z-index: 1000;
            overflow-y: auto;
            padding: 2rem 1.5rem;
            box-shadow: 4px 0 20px rgba(0, 0, 0, 0.05);
        }
        
        .main-content {
            margin-left: 280px;
            min-height: 100vh;
        }
        
        .hero-section {
            background: linear-gradient(135deg, #2c1810 0%, #4a2c1a 50%, #8b7355 100%);
            color: white;
            position: relative;
            overflow: hidden;
        }
        
        .hero-overlay {
            position: absolute;
            inset: 0;
            background: linear-gradient(45deg, rgba(212, 175, 55, 0.1) 0%, transparent 50%);
            mix-blend-mode: overlay;
        }
        
        .bento-grid {
            display: grid;
            grid-template-columns: 2fr 1fr;
            grid-template-rows: auto auto;
            gap: 2rem;
            height: 100%;
        }
        
        .bento-main {
            grid-row: 1 / 3;
            display: flex;
            flex-direction: column;
            justify-content: center;
            padding: 3rem;
        }
        
        .bento-highlight {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(5px);
            border: 1px solid rgba(255, 255, 255, 0.2);
            border-radius: 1rem;
            padding: 2rem;
            margin: 1rem 0;
        }
        
        .section-header {
            border-left: 4px solid var(--accent);
            padding-left: 2rem;
            margin: 4rem 0 2rem 0;
        }
        
        .chart-container {
            background: white;
            border-radius: 1rem;
            padding: 2rem;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
            margin: 2rem 0;
        }
        
        .citation {
            color: var(--accent);
            text-decoration: none;
            font-weight: 500;
            border-bottom: 1px dotted var(--accent);
        }
        
        .citation:hover {
            background: rgba(212, 175, 55, 0.1);
            padding: 0.2rem 0.4rem;
            border-radius: 0.25rem;
        }
        
        .code-block {
            background: #1a1a1a;
            color: #f8f8f2;
            border-radius: 0.75rem;
            padding: 2rem;
            margin: 2rem 0;
            overflow-x: auto;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.9rem;
            line-height: 1.5;
        }
        
        .highlight-box {
            background: linear-gradient(135deg, rgba(212, 175, 55, 0.05) 0%, rgba(139, 115, 85, 0.05) 100%);
            border: 1px solid rgba(212, 175, 55, 0.2);
            border-radius: 1rem;
            padding: 2rem;
            margin: 2rem 0;
        }
        
        .toc-link {
            display: block;
            padding: 0.75rem 1rem;
            color: var(--secondary);
            text-decoration: none;
            border-radius: 0.5rem;
            margin: 0.25rem 0;
            transition: all 0.3s ease;
            font-size: 0.9rem;
        }
        
        .toc-link:hover, .toc-link.active {
            background: rgba(212, 175, 55, 0.1);
            color: var(--primary);
            transform: translateX(0.25rem);
        }
        
        @media (max-width: 1024px) {
            .toc-sidebar { display: none; }
            .main-content { margin-left: 0; }
            .bento-grid { grid-template-columns: 1fr; }
            
            .modal-content {
                width: 95%;
                padding: 1rem;
            }
        }

        /* Media query for small screens */
        @media (max-width: 768px) {
            .hero-section {
                height: auto;
                padding: 2rem 1rem;
            }
            
            .bento-main {
                padding: 1rem;
            }
            
            .hero-section h1 {
                font-size: 2.5rem;
            }
            
            .bento-highlight {
                padding: 1rem;
            }
            
            .hero-section .flex.items-center {
                flex-wrap: wrap;
                gap: 0.5rem;
            }
            
            .section-header {
                padding-left: 1rem;
            }
        }
    </style>
  <base target="_blank">
</head>

  <body class="font-inter">
    <!-- Table of Contents Sidebar -->
    <nav class="toc-sidebar">
      <div class="mb-8">
        <h3 class="font-canela text-xl font-bold text-primary mb-4">Contents</h3>
      </div>

      <div class="space-y-2">
        <a href="#executive-summary" class="toc-link">Executive Summary</a>
        <a href="#spark-fundamentals" class="toc-link">1. Spark Execution Fundamentals</a>
        <div class="ml-4 space-y-1">
          <a href="#distributed-architecture" class="toc-link text-sm">1.1 Distributed Architecture</a>
          <a href="#stages-tasks" class="toc-link text-sm">1.2 Stages &amp; Tasks</a>
          <a href="#dag-overview" class="toc-link text-sm">1.3 DAG Structure</a>
        </div>

        <a href="#understanding-shuffles" class="toc-link">2. Understanding Spark Shuffles</a>
        <div class="ml-4 space-y-1">
          <a href="#shuffle-core" class="toc-link text-sm">2.1 Core Concepts</a>
          <a href="#shuffle-mechanics" class="toc-link text-sm">2.2 Complete Lifecycle</a>
          <a href="#shuffle-evolution" class="toc-link text-sm">2.3 Manager Evolution</a>
        </div>

        <a href="#memory-management" class="toc-link">3. Memory Management &amp; Spillage</a>
        <div class="ml-4 space-y-1">
          <a href="#memory-architecture" class="toc-link text-sm">3.1 Unified Memory</a>
          <a href="#spillage-definition" class="toc-link text-sm">3.2 What is Spillage?</a>
          <a href="#spillage-triggers" class="toc-link text-sm">3.3 Triggers &amp; Mechanisms</a>
          <a href="#spillage-tuning" class="toc-link text-sm">3.4 Configuration &amp; Tuning</a>
        </div>

        <a href="#code-animations" class="toc-link">4. Code-Based Animations</a>
        <div class="ml-4 space-y-1">
          <a href="#animation-framework" class="toc-link text-sm">4.1 Animation Framework</a>
          <a href="#shuffle-animation" class="toc-link text-sm">4.2 Shuffle Process</a>
          <a href="#spillage-animation" class="toc-link text-sm">4.3 Spillage Mechanism</a>
          <a href="#interactive-dag" class="toc-link text-sm">4.4 Interactive DAG</a>
        </div>

        <a href="#simple-dag-demo" class="toc-link">5. Simple DAG Demo</a>
        <div class="ml-4 space-y-1">
          <a href="#demo-scenario" class="toc-link text-sm">5.1 Problem Scenario</a>
          <a href="#pyspark-implementation" class="toc-link text-sm">5.2 PySpark Implementation</a>
          <a href="#visual-dag" class="toc-link text-sm">5.3 Visual DAG</a>
          <a href="#execution-breakdown" class="toc-link text-sm">5.4 Execution Breakdown</a>
          <a href="#performance-analysis" class="toc-link text-sm">5.5 Performance Analysis</a>
        </div>

        <a href="#optimization-strategies" class="toc-link">6. Optimization Strategies</a>
        <div class="ml-4 space-y-1">
          <a href="#minimizing-shuffle" class="toc-link text-sm">6.1 Minimizing Shuffles</a>
          <a href="#preventing-spillage" class="toc-link text-sm">6.2 Preventing Spillage</a>
          <a href="#monitoring-diagnostics" class="toc-link text-sm">6.3 Monitoring &amp; Diagnostics</a>
        </div>
      </div>
    </nav>

    <!-- Main Content -->
    <main class="main-content">
      <!-- Hero Section -->
      <section class="hero-section min-h-screen flex items-center">
        <div class="hero-overlay"></div>
        <div class="container mx-auto px-8 relative z-10">
          <div class="bento-grid max-w-7xl mx-auto">
            <div class="bento-main">
              <h1 class="font-canela text-6xl md:text-7xl font-bold mb-8 leading-tight">
                <span class="italic text-yellow-200">Apache Spark</span>
                <br/>
                End-to-End Execution
              </h1>
              <p class="text-xl md:text-2xl text-gray-200 mb-8 max-w-3xl leading-relaxed">
                A comprehensive exploration of shuffle mechanics, memory management,
                and distributed data processing internals
              </p>
              <div class="flex items-center space-x-6 text-gray-300">
                <span class="flex items-center"><i class="fas fa-database mr-2"></i>Distributed Computing</span>
                <span class="flex items-center"><i class="fas fa-memory mr-2"></i>Memory Optimization</span>
                <span class="flex items-center"><i class="fas fa-network-wired mr-2"></i>Data Shuffling</span>
              </div>
            </div>

            <div class="space-y-6">
              <div class="bento-highlight">
                <h3 class="font-canela text-xl font-bold mb-3">Key Insights</h3>
                <ul class="space-y-2 text-sm">
                  <li>• Shuffles are the primary performance bottleneck</li>
                  <li>• Spillage indicates memory pressure and misconfiguration</li>
                  <li>• Unified memory management enables dynamic optimization</li>
                </ul>
              </div>

              <div class="bento-highlight">
                <h3 class="font-canela text-xl font-bold mb-3">Practical Applications</h3>
                <ul class="space-y-2 text-sm">
                  <li>• Large-scale ETL pipeline optimization</li>
                  <li>• Machine learning feature engineering</li>
                  <li>• Real-time stream processing</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Executive Summary -->
      <section id="executive-summary" class="py-16 bg-white">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="section-header">
            <h2 class="font-canela text-4xl font-bold text-primary mb-6">Executive Summary</h2>
          </div>

          <div class="highlight-box">
            <p class="text-lg leading-relaxed mb-6">
              Apache Spark&#39;s shuffle mechanism represents the <strong>most critical performance bottleneck</strong>
              in distributed data processing. This comprehensive analysis reveals that shuffle operations—
              triggered by key-based transformations like
              <code>groupBy</code> and
              <code>join</code>—involve
              complex all-to-all data redistribution across cluster nodes, consuming significant network,
              disk, and memory resources.
            </p>

            <p class="text-lg leading-relaxed mb-6">
              The research demonstrates that <strong>spillage</strong>, Spark&#39;s emergency response to memory
              pressure, fundamentally differs from intentional shuffle operations. While shuffle is a
              planned computational pattern, spill represents misconfiguration that can degrade performance
              by 10-100x through unnecessary disk I/O operations.
            </p>

            <p class="text-lg leading-relaxed">
              Modern Spark&#39;s <a href="https://zhmin.github.io/posts/spark-shuffle-sort-writer-2/" class="citation">sort-based shuffle implementation</a>
              with unified memory management provides robust foundations for large-scale data processing,
              but optimal performance requires deep understanding of these internal mechanisms and careful
              tuning of partition counts, memory allocation, and transformation strategies.
            </p>
          </div>
        </div>
      </section>

      <!-- Spark Execution Fundamentals -->
      <section id="spark-fundamentals" class="py-16 bg-neutral">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="section-header">
            <h2 class="font-canela text-4xl font-bold text-primary mb-6">1. Spark Execution Fundamentals</h2>
          </div>

          <!-- Distributed Architecture -->
          <div id="distributed-architecture" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">1.1 Distributed Computing Architecture</h3>

            <div class="grid md:grid-cols-2 gap-8 mb-8">
              <div>
                <h4 class="font-inter text-xl font-semibold mb-4">Driver-Executor Model</h4>
                <p class="mb-4">
                  Spark operates on a <strong>master-worker paradigm</strong> where the <strong>Driver</strong>
                  serves as central coordinator and <strong>Executors</strong> perform data processing across
                  cluster nodes. The Driver hosts the <a href="https://stackoverflow.com/questions/25836316/how-dag-works-under-the-covers-in-rdd" class="citation">SparkContext</a>,
                  maintaining global execution state and orchestrating the entire DAG.
                </p>

                <p class="mb-4">
                  Executors run in their own JVM processes with dedicated memory and CPU cores, processing
                  data partitions independently. The cluster manager—whether YARN, Kubernetes, Mesos, or
                  Standalone—handles resource allocation while the Driver maintains logical control.
                </p>
              </div>

              <div>
                <img src="https://kimi-web-img.moonshot.cn/img/placeholder-0620/图片3.png" alt="Apache Spark driver and executors cluster architecture" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" query="Apache Spark cluster architecture diagram" referrerpolicy="no-referrer" data-modified="1"/>
              </div>
            </div>

            <div class="highlight-box">
              <h4 class="font-inter text-xl font-semibold mb-4">Role of DAGScheduler</h4>
              <p class="mb-4">
                The <strong>DAGScheduler</strong> transforms high-level RDD lineage into stages of physical tasks.
                When an action triggers execution, the scheduler analyzes dependency graphs to identify
                <strong>stage boundaries</strong> at wide dependencies (shuffles). It pipelines narrow
                transformations within each stage to minimize task overhead.
              </p>

              <p>
                The scheduler performs <a href="https://blog.devgenius.io/mastering-spark-dags-the-ultimate-guide-to-understanding-execution-ce6683ae785b" class="citation">reverse traversal</a>
                from the action RDD, accumulating narrow dependencies into stages until encountering shuffle
                boundaries, creating a stage DAG with correct execution order.
              </p>
            </div>
          </div>

          <!-- Stages and Tasks -->
          <div id="stages-tasks" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">1.2 Stages and Tasks: The Building Blocks</h3>

            <div class="grid md:grid-cols-3 gap-6 mb-8">
              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-lg font-semibold mb-3">Narrow Dependencies</h4>
                <p class="text-sm mb-3">Operations like map, filter, flatMap that process data within existing partitions without requiring data movement.</p>
                <div class="text-xs text-gray-600">
                  <strong>Examples:</strong> map, filter, flatMap, mapPartitions, union
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-lg font-semibold mb-3">Wide Dependencies</h4>
                <p class="text-sm mb-3">Operations requiring data redistribution across partitions, triggering shuffle and stage boundaries.</p>
                <div class="text-xs text-gray-600">
                  <strong>Examples:</strong> groupBy, join, sortByKey, repartition
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-lg font-semibold mb-3">Task Execution</h4>
                <p class="text-sm mb-3">The smallest execution unit processing individual data partitions, distributed across executors.</p>
                <div class="text-xs text-gray-600">
                  <strong>Parallelism:</strong> Limited by executor cores and partition count
                </div>
              </div>
            </div>

            <div class="chart-container">
              <h4 class="font-inter text-xl font-semibold mb-4">Transformation Dependency Types</h4>
              <div class="overflow-x-auto">
                <table class="w-full text-sm">
                  <thead>
                    <tr class="border-b border-gray-200">
                      <th class="text-left py-3 px-4 font-semibold">Transformation</th>
                      <th class="text-left py-3 px-4 font-semibold">Dependency Type</th>
                      <th class="text-left py-3 px-4 font-semibold">Shuffle Required</th>
                      <th class="text-left py-3 px-4 font-semibold">Key Characteristic</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-gray-100">
                    <tr>
                      <td class="py-3 px-4 font-mono">map, filter, flatMap</td>
                      <td class="py-3 px-4"><span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs">Narrow</span></td>
                      <td class="py-3 px-4">No</td>
                      <td class="py-3 px-4">One-to-one partition mapping</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">union, sample</td>
                      <td class="py-3 px-4"><span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs">Narrow</span></td>
                      <td class="py-3 px-4">No</td>
                      <td class="py-3 px-4">Many-to-one partition mapping</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">groupBy, reduceByKey</td>
                      <td class="py-3 px-4"><span class="bg-red-100 text-red-800 px-2 py-1 rounded text-xs">Wide</span></td>
                      <td class="py-3 px-4">Yes</td>
                      <td class="py-3 px-4">Key-based grouping across partitions</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">join (no broadcast)</td>
                      <td class="py-3 px-4"><span class="bg-red-100 text-red-800 px-2 py-1 rounded text-xs">Wide</span></td>
                      <td class="py-3 px-4">Yes</td>
                      <td class="py-3 px-4">Both datasets redistributed on join key</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">repartition</td>
                      <td class="py-3 px-4"><span class="bg-red-100 text-red-800 px-2 py-1 rounded text-xs">Wide</span></td>
                      <td class="py-3 px-4">Yes</td>
                      <td class="py-3 px-4">Explicit redistribution</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">coalesce (decrease)</td>
                      <td class="py-3 px-4"><span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs">Narrow</span></td>
                      <td class="py-3 px-4">No</td>
                      <td class="py-3 px-4">Merge partitions locally</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <p class="mt-6 text-sm text-gray-600">
              Source: <a href="https://developer.hpe.com/blog/how-spark-runs-your-applications/" class="citation">Spark Execution Architecture</a>
            </p>
          </div>

          <!-- DAG Overview -->
          <div id="dag-overview" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">1.3 The DAG (Directed Acyclic Graph)</h3>

            <div class="bg-white p-8 rounded-lg shadow-lg mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">RDD Lineage and Lazy Evaluation</h4>
              <p class="mb-4">
                Spark&#39;s <strong>lazy evaluation</strong> model defers computation until an action is invoked,
                recording transformations in a <strong>DAG</strong> that represents the complete computation plan.
                Each RDD maintains <a href="https://stackoverflow.com/questions/25836316/how-dag-works-under-the-covers-in-rdd" class="citation">pointers to parent RDDs</a>
                with transformation metadata—this lineage enables both optimization and fault recovery.
              </p>

              <p>
                The DAG structure ensures no circular dependencies exist: transformations always move forward
                from source data to derived results, never cycling back. This immutability and lineage
                provide the foundation for Spark&#39;s fault tolerance through recomputation.
              </p>
            </div>

            <div class="grid md:grid-cols-2 gap-8">
              <div>
                <h4 class="font-inter text-xl font-semibold mb-4">Logical to Physical Plan Transformation</h4>
                <p class="mb-4">
                  Spark&#39;s query execution proceeds through multiple planning phases. The <strong>logical plan</strong>
                  represents abstract computation, while the <strong>physical plan</strong> specifies concrete
                  execution strategies through the <a href="https://blog.devgenius.io/mastering-spark-dags-the-ultimate-guide-to-understanding-execution-ce6683ae785b" class="citation">Catalyst optimizer</a>.
                </p>

                <div class="text-sm space-y-2">
                  <div class="flex items-center">
                    <span class="bg-blue-100 text-blue-800 px-2 py-1 rounded text-xs mr-3">Analysis</span>
                    Resolve column references, validate schemas
                  </div>
                  <div class="flex items-center">
                    <span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs mr-3">Logical Optimization</span>
                    Predicate pushdown, constant folding
                  </div>
                  <div class="flex items-center">
                    <span class="bg-purple-100 text-purple-800 px-2 py-1 rounded text-xs mr-3">Physical Planning</span>
                    Join strategy selection, broadcast hints
                  </div>
                  <div class="flex items-center">
                    <span class="bg-orange-100 text-orange-800 px-2 py-1 rounded text-xs mr-3">Code Generation</span>
                    Whole-stage code generation (WSCG)
                  </div>
                </div>
              </div>

              <div>
                <img src="https://kimi-web-img.moonshot.cn/img/www.nvidia.com/abca2895568e0091643da1d703acf8246766a5fb.png" alt="Spark query plan transformation from logical to physical with optimization stages" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" style="linedrawing" query="Apache Spark query plan transformation diagram" referrerpolicy="no-referrer" data-modified="1" data-score="0.00"/>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Understanding Spark Shuffles -->
      <section id="understanding-shuffles" class="py-16 bg-white">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="section-header">
            <h2 class="font-canela text-4xl font-bold text-primary mb-6">2. Understanding Spark Shuffles</h2>
          </div>

          <!-- Shuffle Core Concepts -->
          <div id="shuffle-core" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">2.1 What Is a Shuffle: Core Concepts</h3>

            <div class="highlight-box mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Definition: Data Redistribution Across Partitions</h4>
              <p class="mb-4">
                A <strong>shuffle</strong> in Apache Spark is the <strong>all-to-all data redistribution operation</strong>
                that occurs when data must be reorganized across partitions based on key values. Unlike narrow
                transformations, shuffles physically move records between executors—often across network boundaries—to
                ensure that all records sharing a common key become colocated in the same destination partition.
              </p>

              <p>
                The <a href="http://www.ce.uniroma2.it/courses/sabd2324/slides/Spark.pdf" class="citation">shuffle mechanism</a>
                implements a distributed sorting and exchange problem: given M map tasks and R reduce tasks,
                every record from every map task must reach the correct reduce task based on its key&#39;s hash value.
              </p>
            </div>

            <div class="chart-container mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Shuffle Resource Impact Matrix</h4>
              <div class="overflow-x-auto">
                <table class="w-full text-sm">
                  <thead>
                    <tr class="border-b border-gray-200">
                      <th class="text-left py-3 px-4 font-semibold">Resource</th>
                      <th class="text-left py-3 px-4 font-semibold">Shuffle Impact</th>
                      <th class="text-left py-3 px-4 font-semibold">Mitigation Strategies</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-gray-100">
                    <tr>
                      <td class="py-3 px-4 font-medium">Network</td>
                      <td class="py-3 px-4">All-to-all data transfer; O(M×R) connections</td>
                      <td class="py-3 px-4">Compression, locality-aware scheduling, push-based shuffle</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Disk I/O</td>
                      <td class="py-3 px-4">Shuffle file write (map) and read (reduce); random I/O patterns</td>
                      <td class="py-3 px-4">SSDs, sequential I/O optimization, shuffle file consolidation</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Memory</td>
                      <td class="py-3 px-4">Buffer management for partitioning; spill handling</td>
                      <td class="py-3 px-4">Increased executor memory, tuned buffer sizes, off-heap allocation</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">CPU</td>
                      <td class="py-3 px-4">Serialization/deserialization; sorting; compression</td>
                      <td class="py-3 px-4">Optimized serializers (Kryo), columnar formats, vectorized execution</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <p class="mt-4 text-sm text-gray-600">
                Source: <a href="https://www.semanticscholar.org/paper/Optimizing-Shuffle-Performance-in-Spark-Davidson/d746505bad055c357fa50d394d15eb380a3f1ad3" class="citation">Optimizing Shuffle Performance in Spark</a>
              </p>
            </div>

            <div class="grid md:grid-cols-2 gap-8">
              <div>
                <h4 class="font-inter text-xl font-semibold mb-4">When Shuffles Occur: Key-Triggered Operations</h4>
                <p class="mb-4">
                  Shuffles are triggered by specific transformation types that require key-based data colocation.
                  The most common operations include grouping/aggregation, sorting, set operations, joins, and
                  explicit repartitioning.
                </p>

                <div class="space-y-3 text-sm">
                  <div class="flex items-start">
                    <span class="bg-blue-100 text-blue-800 px-2 py-1 rounded text-xs mr-3 mt-0.5">GroupBy</span>
                    <span>Hash partition by key, group values per partition</span>
                  </div>
                  <div class="flex items-start">
                    <span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs mr-3 mt-0.5">SortBy</span>
                    <span>Range partition or hash partition with sort</span>
                  </div>
                  <div class="flex items-start">
                    <span class="bg-purple-100 text-purple-800 px-2 py-1 rounded text-xs mr-3 mt-0.5">Join</span>
                    <span>Both datasets redistributed on join key</span>
                  </div>
                  <div class="flex items-start">
                    <span class="bg-orange-100 text-orange-800 px-2 py-1 rounded text-xs mr-3 mt-0.5">Distinct</span>
                    <span>Hash partition to identify duplicates across partitions</span>
                  </div>
                </div>
              </div>

              <div>
                <h4 class="font-inter text-xl font-semibold mb-4">Shuffle as Primary Performance Bottleneck</h4>
                <p class="mb-4">
                  Industry research consistently identifies shuffle operations as the <strong>dominant performance bottleneck</strong>
                  in Spark workloads. The <a href="https://www.semanticscholar.org/paper/Optimizing-Shuffle-Performance-in-Spark-Davidson/d746505bad055c357fa50d394d15eb380a3f1ad3" class="citation">UC Berkeley research</a>
                  established foundational understanding of shuffle costs across network, disk, memory, and CPU dimensions.
                </p>

                <div class="bg-red-50 border-l-4 border-red-400 p-4 rounded">
                  <p class="text-sm text-red-800">
                    <strong>Critical Insight:</strong> The severity of shuffle overhead scales with cluster and data size.
                    A job processing 1TB with 1000 partitions may generate millions of shuffle files, creating metadata
                    pressure and I/O scheduling challenges.
                  </p>
                </div>
              </div>
            </div>
          </div>

          <!-- Shuffle Mechanics -->
          <div id="shuffle-mechanics" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">2.2 Shuffle Mechanics: The Complete Lifecycle</h3>

            <div class="grid md:grid-cols-3 gap-6 mb-8">
              <div class="bg-gradient-to-br from-blue-50 to-blue-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-blue-900">Map Phase</h4>
                <ul class="text-sm space-y-2 text-blue-800">
                  <li>• Read input partitions</li>
                  <li>• Apply transformations</li>
                  <li>• Hash-based partitioning</li>
                  <li>• In-memory buffering</li>
                  <li>• Spill to disk if needed</li>
                </ul>
              </div>

              <div class="bg-gradient-to-br from-green-50 to-green-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-green-900">Materialization</h4>
                <ul class="text-sm space-y-2 text-green-800">
                  <li>• Data file generation</li>
                  <li>• Index file creation</li>
                  <li>• External sorting</li>
                  <li>• Spill file merging</li>
                  <li>• Shuffle metadata tracking</li>
                </ul>
              </div>

              <div class="bg-gradient-to-br from-purple-50 to-purple-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-purple-900">Reduce Phase</h4>
                <ul class="text-sm space-y-2 text-purple-800">
                  <li>• Block location resolution</li>
                  <li>• Network fetch requests</li>
                  <li>• Merge-sort of data</li>
                  <li>• Final aggregation</li>
                  <li>• Output generation</li>
                </ul>
              </div>
            </div>

            <div class="mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Map Phase: Partitioning and Bucket Creation</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <p class="mb-4">
                    The <strong>map phase</strong> begins with each map task reading its assigned input partition
                    and applying upstream narrow transformations. Records flow through the transformation chain
                    without materialization until reaching the shuffle write process.
                  </p>

                  <div class="code-block mb-4">
                    <pre># Hash-based partitioning logic
partitionId = hash(key) % numPartitions

# numPartitions typically from:
# - spark.sql.shuffle.partitions (default 200)
# - Explicit partition count for RDD operations</pre>
                  </div>

                  <p class="text-sm text-gray-600">
                    Source: <a href="https://zhuanlan.zhihu.com/p/1977762222299189683" class="citation">Spark Shuffle Implementation Deep Dive</a>
                  </p>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">Memory Management During Map Phase</h5>
                  <ul class="space-y-2 text-sm">
                    <li class="flex items-start">
                      <i class="fas fa-memory text-blue-500 mr-2 mt-1"></i>
                      <span><strong>Shuffle buffer:</strong> 32KB per file output stream (<code>spark.shuffle.file.buffer</code>)</span>
                    </li>
                    <li class="flex items-start">
                      <i class="fas fa-sort text-green-500 mr-2 mt-1"></i>
                      <span><strong>Sort threshold:</strong> Triggers spill when memory pressure detected</span>
                    </li>
                    <li class="flex items-start">
                      <i class="fas fa-database text-purple-500 mr-2 mt-1"></i>
                      <span><strong>Spill management:</strong> External sorting for datasets exceeding memory</span>
                    </li>
                  </ul>

                  <div class="mt-4 p-3 bg-yellow-50 border-l-4 border-yellow-400 rounded">
                    <p class="text-sm text-yellow-800">
                      <strong>Key Point:</strong> Spilling is not an emergency but a standard operational pattern
                      for large shuffles, enabling processing of arbitrarily large datasets.
                    </p>
                  </div>
                </div>
              </div>
            </div>

            <div class="mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Intermediate Data Materialization</h4>

              <p class="mb-6">
                The sort-based shuffle implementation produces <strong>two files per map task</strong>:
                a <strong>data file</strong> containing all partitioned records, and an <strong>index file</strong>
                recording byte offsets for each partition&#39;s data. This design consolidates what would be
                R separate files in hash shuffle into a single file, dramatically reducing filesystem metadata pressure.
              </p>

              <div class="chart-container">
                <h5 class="font-inter text-lg font-semibold mb-4">File Structure Comparison: Hash vs. Sort Shuffle</h5>
                <div class="overflow-x-auto">
                  <table class="w-full text-sm">
                    <thead>
                      <tr class="border-b border-gray-200">
                        <th class="text-left py-3 px-4 font-semibold">Scenario</th>
                        <th class="text-left py-3 px-4 font-semibold">Map Tasks</th>
                        <th class="text-left py-3 px-4 font-semibold">Reduce Tasks</th>
                        <th class="text-left py-3 px-4 font-semibold">Hash Shuffle Files</th>
                        <th class="text-left py-3 px-4 font-semibold">Sort Shuffle Files</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-gray-100">
                      <tr>
                        <td class="py-3 px-4">Small job</td>
                        <td class="py-3 px-4">10</td>
                        <td class="py-3 px-4">10</td>
                        <td class="py-3 px-4"><span class="text-red-600 font-semibold">100</span></td>
                        <td class="py-3 px-4"><span class="text-green-600 font-semibold">20</span></td>
                      </tr>
                      <tr>
                        <td class="py-3 px-4">Medium job</td>
                        <td class="py-3 px-4">100</td>
                        <td class="py-3 px-4">200</td>
                        <td class="py-3 px-4"><span class="text-red-600 font-semibold">20,000</span></td>
                        <td class="py-3 px-4"><span class="text-green-600 font-semibold">200</span></td>
                      </tr>
                      <tr>
                        <td class="py-3 px-4">Large job</td>
                        <td class="py-3 px-4">1,000</td>
                        <td class="py-3 px-4">1,000</td>
                        <td class="py-3 px-4"><span class="text-red-600 font-semibold">1,000,000</span></td>
                        <td class="py-3 px-4"><span class="text-green-600 font-semibold">2,000</span></td>
                      </tr>
                      <tr>
                        <td class="py-3 px-4">Very large job</td>
                        <td class="py-3 px-4">10,000</td>
                        <td class="py-3 px-4">2,000</td>
                        <td class="py-3 px-4"><span class="text-red-600 font-semibold">20,000,000</span></td>
                        <td class="py-3 px-4"><span class="text-green-600 font-semibold">20,000</span></td>
                      </tr>
                    </tbody>
                  </table>
                </div>
                <p class="mt-4 text-sm text-gray-600">
                  Source: <a href="https://0x0fff.com/spark-architecture-shuffle/" class="citation">Spark Architecture and Shuffle Design</a>
                </p>
              </div>
            </div>

            <div>
              <h4 class="font-inter text-xl font-semibold mb-4">Reduce Phase: Data Fetching and Aggregation</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <p class="mb-4">
                    When the map stage completes, the Driver collects metadata about all shuffle outputs
                    through the <strong>MapOutputTracker</strong> service. Reduce tasks query this service
                    to discover block locations before initiating fetch requests.
                  </p>

                  <div class="space-y-3">
                    <div class="flex items-start">
                      <i class="fas fa-search text-blue-500 mr-2 mt-1"></i>
                      <span class="text-sm"><strong>Location resolution:</strong> Centralized metadata management enables dynamic scheduling</span>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-network-wired text-green-500 mr-2 mt-1"></i>
                      <span class="text-sm"><strong>Network fetch:</strong> Parallel fetching with batched requests and compression</span>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-merge text-purple-500 mr-2 mt-1"></i>
                      <span class="text-sm"><strong>Data merge:</strong> External merge-sort combines multiple map outputs</span>
                    </div>
                  </div>
                </div>

                <div>
                  <div class="code-block">
                    <pre># Reduce task fetch process
1. Query MapOutputTracker for block locations
2. Initiate parallel fetch requests (5 concurrent)
3. Apply compression (LZ4/Snappy)
4. Bypass network for local data
5. Merge-sort fetched data
6. Apply final aggregation</pre>
                  </div>

                  <div class="mt-4 p-3 bg-blue-50 border-l-4 border-blue-400 rounded">
                    <p class="text-sm text-blue-800">
                      <strong>Optimization:</strong> The default
                      <code>spark.reducer.maxSizeInFlight</code> (48MB) enables
                      batched requests that amortize connection overhead across large data transfers.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Shuffle Evolution -->
          <div id="shuffle-evolution" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">2.3 Shuffle Manager Evolution</h3>

            <div class="grid md:grid-cols-2 gap-8 mb-8">
              <div class="bg-red-50 p-6 rounded-lg border border-red-200">
                <h4 class="font-inter text-xl font-semibold mb-4 text-red-900">Hash Shuffle: Historical Limitations</h4>
                <p class="mb-4 text-red-800">
                  Spark&#39;s original hash shuffle produced <strong>M × R shuffle files</strong>—a quadratic
                  growth pattern that overwhelmed filesystem capabilities and created severe scalability limits.
                </p>

                <ul class="space-y-2 text-sm text-red-700">
                  <li>• HDFS NameNode memory pressure from excessive metadata</li>
                  <li>• OS limits on open file descriptors</li>
                  <li>• Random I/O patterns reducing disk throughput by 100x</li>
                  <li>• I/O scheduler inefficiency with millions of files</li>
                </ul>
              </div>

              <div class="bg-green-50 p-6 rounded-lg border border-green-200">
                <h4 class="font-inter text-xl font-semibold mb-4 text-green-900">Sort-Based Shuffle: Current Default</h4>
                <p class="mb-4 text-green-800">
                  Introduced in Spark 1.2, sort-based shuffle produces <strong>2M total files</strong>
                  (M data + M index) regardless of reduce task count, representing a 100× improvement over hash shuffle.
                </p>

                <ul class="space-y-2 text-sm text-green-700">
                  <li>• Sequential I/O patterns maximizing disk throughput</li>
                  <li>• External sorting for datasets exceeding memory</li>
                  <li>• Efficient compression with contiguous data</li>
                  <li>• Index files enabling precise random access</li>
                </ul>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg border border-gray-200">
              <h4 class="font-inter text-xl font-semibold mb-4">External Shuffle Service (ESS) and Push-Based Shuffle</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <p class="mb-4">
                    The <strong>External Shuffle Service (ESS)</strong> runs as a separate process on worker nodes,
                    <a href="http://www.vldb.org/pvldb/vol13/p3382-shen.pdf" class="citation">decoupling shuffle data from executor lifecycle</a>.
                    This enables shuffle data persistence even if executors are lost, supporting dynamic allocation
                    and graceful degradation during failures.
                  </p>

                  <div class="bg-blue-50 p-4 rounded">
                    <h5 class="font-semibold text-blue-900 mb-2">ESS Benefits</h5>
                    <ul class="text-sm text-blue-800 space-y-1">
                      <li>• Executor loss doesn&#39;t invalidate shuffle data</li>
                      <li>• Supports dynamic allocation and scaling</li>
                      <li>• Essential for cloud deployments with preemption</li>
                      <li>• Independent process from executor JVM</li>
                    </ul>
                  </div>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">Magnet: Push-Merge Shuffle</h5>
                  <p class="mb-4 text-sm">
                    LinkedIn&#39;s <strong>Magnet</strong> project extends ESS with push-merge shuffle,
                    decoupling shuffle data transfer from map task completion through early block push
                    and remote merging.
                  </p>

                  <div class="space-y-2 text-xs">
                    <div><strong>Early block push:</strong> Overlaps shuffle transfer with map computation</div>
                    <div><strong>Remote merging:</strong> Consolidates blocks at destination ESS</div>
                    <div><strong>Best-effort operation:</strong> Tolerates partial push failures</div>
                    <div><strong>Improved read locality:</strong> Merged files colocated with reducers</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Memory Management and Spillage -->
      <section id="memory-management" class="py-16 bg-neutral">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="section-header">
            <h2 class="font-canela text-4xl font-bold text-primary mb-6">3. Memory Management and Spillage</h2>
          </div>

          <!-- Memory Architecture -->
          <div id="memory-architecture" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">3.1 Unified Memory Management Architecture</h3>

            <div class="chart-container mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Spark Memory Allocation Model</h4>
              <div class="bg-white p-6 rounded-lg shadow-inner">
                <div class="relative h-64">
                  <!-- Memory visualization bars -->
                  <div class="absolute inset-0 flex flex-col justify-center">
                    <div class="mb-4">
                      <div class="flex items-center justify-between mb-2">
                        <span class="text-sm font-medium">Reserved Memory (Spark Internal)</span>
                        <span class="text-sm text-gray-500">~300MB fixed</span>
                      </div>
                      <div class="w-full bg-gray-200 rounded-full h-6">
                        <div class="bg-gray-600 h-6 rounded-full" style="width: 5%"></div>
                      </div>
                    </div>

                    <div class="mb-4">
                      <div class="flex items-center justify-between mb-2">
                        <span class="text-sm font-medium">User Memory (UDFs, user data)</span>
                        <span class="text-sm text-gray-500">40% of heap</span>
                      </div>
                      <div class="w-full bg-gray-200 rounded-full h-6">
                        <div class="bg-blue-500 h-6 rounded-full" style="width: 40%"></div>
                      </div>
                    </div>

                    <div>
                      <div class="flex items-center justify-between mb-2">
                        <span class="text-sm font-medium">Unified Memory (Execution + Storage)</span>
                        <span class="text-sm text-gray-500">60% of heap</span>
                      </div>
                      <div class="w-full bg-gray-200 rounded-full h-8">
                        <div class="bg-gradient-to-r from-green-400 to-yellow-400 h-8 rounded-full" style="width: 60%"></div>
                      </div>
                      <div class="flex justify-between text-xs text-gray-500 mt-1">
                        <span>Execution (computation)</span>
                        <span>Storage (caching)</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              <p class="mt-4 text-sm text-gray-600">
                Configuration:
                <code>spark.memory.fraction=0.6</code> (default),
                <code>spark.memory.storageFraction=0.5</code> (initial boundary)
              </p>
            </div>

            <div class="grid md:grid-cols-2 gap-8">
              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Execution Memory vs. Storage Memory</h4>
                <p class="mb-4">
                  <a href="https://www.linkedin.com/pulse/understanding-memory-spills-apachespark-shanoj-kumar-v-ifywc" class="citation">Unified Memory Management</a>
                  consolidates memory allocation into a shared pool, enabling dynamic reallocation based on workload demands.
                </p>

                <div class="space-y-3">
                  <div>
                    <h5 class="font-semibold text-sm mb-1">Execution Memory</h5>
                    <p class="text-xs text-gray-600">Supports computation: shuffle sorting, hash tables for aggregations, join buffers</p>
                  </div>
                  <div>
                    <h5 class="font-semibold text-sm mb-1">Storage Memory</h5>
                    <p class="text-xs text-gray-600">Holds cached RDDs/DataFrames and broadcast variables</p>
                  </div>
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Dynamic Occupancy and Eviction</h4>
                <p class="mb-4">
                  The unified pool implements dynamic occupancy with asymmetric eviction policies reflecting
                  different recomputation costs.
                </p>

                <div class="space-y-2 text-sm">
                  <div class="flex items-center">
                    <i class="fas fa-arrow-down text-red-500 mr-2"></i>
                    <span>Storage can be evicted for execution needs</span>
                  </div>
                  <div class="flex items-center">
                    <i class="fas fa-shield text-green-500 mr-2"></i>
                    <span>Execution cannot be evicted by storage</span>
                  </div>
                  <div class="flex items-center">
                    <i class="fas fa-database text-blue-500 mr-2"></i>
                    <span>Storage can spill to disk if eviction insufficient</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Spillage Definition -->
          <div id="spillage-definition" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">3.2 What Is Spillage: Emergency Memory Relief</h3>

            <div class="highlight-box mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Definition: Writing Overflow Data to Disk</h4>
              <p class="mb-4">
                <strong>Spillage</strong> occurs when Spark&#39;s in-memory data structures exceed available memory,
                forcing data to be written to disk to prevent <strong>OutOfMemory errors</strong>. Unlike shuffle—which
                is intentional data movement—spill is an <strong>emergency response to memory pressure</strong>.
              </p>

              <p>
                <a href="https://stackoverflow.com/questions/37848182/understanding-spark-shuffle-spill" class="citation">Spill represents performance degradation</a>:
                disk I/O is orders of magnitude slower than memory access, and spilled data must eventually be read back,
                doubling the I/O cost.
              </p>
            </div>

            <div class="chart-container mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Spillage vs. Shuffle: Key Distinction</h4>
              <div class="overflow-x-auto">
                <table class="w-full text-sm">
                  <thead>
                    <tr class="border-b border-gray-200">
                      <th class="text-left py-3 px-4 font-semibold">Aspect</th>
                      <th class="text-left py-3 px-4 font-semibold">Shuffle</th>
                      <th class="text-left py-3 px-4 font-semibold">Spill</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-gray-100">
                    <tr>
                      <td class="py-3 px-4 font-medium">Intent</td>
                      <td class="py-3 px-4">Intentional data redistribution for computation</td>
                      <td class="py-3 px-4">Unintentional overflow response to memory pressure</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Trigger</td>
                      <td class="py-3 px-4">Wide transformation in query plan</td>
                      <td class="py-3 px-4">Memory limit exceeded during execution</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Timing</td>
                      <td class="py-3 px-4">Predictable, at stage boundaries</td>
                      <td class="py-3 px-4">Unpredictable, during task execution</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Optimization</td>
                      <td class="py-3 px-4">Query plan optimization, partition tuning</td>
                      <td class="py-3 px-4">Memory allocation, partition sizing</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Spark UI Indicator</td>
                      <td class="py-3 px-4">&#34;Shuffle Write&#34; / &#34;Shuffle Read&#34; metrics</td>
                      <td class="py-3 px-4">&#34;Spill (Memory)&#34; / &#34;Spill (Disk)&#34; in task details</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <p class="mt-4 text-sm text-gray-600">
                Source: <a href="https://www.linkedin.com/pulse/understanding-memory-spills-apachespark-shanoj-kumar-v-ifywc" class="citation">Understanding Memory Spills in Apache Spark</a>
              </p>
            </div>
          </div>

          <!-- Spillage Triggers -->
          <div id="spillage-triggers" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">3.3 Spillage Triggers and Mechanisms</h3>

            <div class="grid md:grid-cols-3 gap-6 mb-8">
              <div class="bg-white p-6 rounded-lg shadow-lg border-l-4 border-blue-500">
                <h4 class="font-inter text-lg font-semibold mb-3">Execution Memory Spill</h4>
                <p class="text-sm mb-3">During aggregations using AppendOnlyMap structures</p>
                <ul class="text-xs space-y-1 text-gray-600">
                  <li>• Memory threshold checking every 32 records</li>
                  <li>• Spill-to-disk with sorted temporary files</li>
                  <li>• K-way merge for final aggregation</li>
                </ul>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg border-l-4 border-green-500">
                <h4 class="font-inter text-lg font-semibold mb-3">Shuffle Write Spill</h4>
                <p class="text-sm mb-3">When shuffle buffers exceed 5MB threshold</p>
                <ul class="text-xs space-y-1 text-gray-600">
                  <li>• Default buffer: 5MB (configurable)</li>
                  <li>• Sort by partition ID before writing</li>
                  <li>• Final merge before reduce fetch</li>
                </ul>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg border-l-4 border-purple-500">
                <h4 class="font-inter text-lg font-semibold mb-3">Storage Memory Spill</h4>
                <p class="text-sm mb-3">When cached data is evicted to disk</p>
                <ul class="text-xs space-y-1 text-gray-600">
                  <li>• LRU eviction policy</li>
                  <li>• Serialized to disk with compression</li>
                  <li>• Deserialization costs on read-back</li>
                </ul>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Detailed Spill Mechanisms</h4>

              <div class="space-y-8">
                <div>
                  <h5 class="font-inter font-semibold mb-3 text-lg">Execution Memory Spill During Aggregations</h5>
                  <p class="mb-4">
                    Spark&#39;s aggregation operations use specialized <strong>AppendOnlyMap</strong> structures—
                    open-addressed hash tables optimized for the aggregation pattern. When memory pressure is detected,
                    the map&#39;s contents are sorted and written to disk, then cleared for continued processing.
                  </p>

                  <div class="grid md:grid-cols-2 gap-6">
                    <div>
                      <h6 class="font-semibold mb-2">Memory Threshold Checking</h6>
                      <ul class="text-sm space-y-1">
                        <li>• Checks occur <strong>every 32 records</strong> by default</li>
                        <li>• Evaluates current memory allocation against limits</li>
                        <li>• Estimates in-memory structure sizes and growth rates</li>
                        <li>• Triggers spill if thresholds exceeded</li>
                      </ul>
                    </div>

                    <div>
                      <h6 class="font-semibold mb-2">Spill and Merge Process</h6>
                      <ul class="text-sm space-y-1">
                        <li>• Sort in-memory records by key for efficient merge</li>
                        <li>• Write to temporary file with compression</li>
                        <li>• Clear in-memory structure for continued processing</li>
                        <li>• K-way merge algorithm for final aggregation</li>
                      </ul>
                    </div>
                  </div>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3 text-lg">Shuffle Write Spill</h5>
                  <p class="mb-4">
                    Shuffle write operations maintain in-memory buffers for partitioning and sorting.
                    The <strong>default 5MB threshold</strong> (
                    <code>spark.shuffle.spill.initialMemoryThreshold</code>) determines when spill is triggered.
                  </p>

                  <div class="bg-gray-50 p-4 rounded">
                    <h6 class="font-semibold mb-2">Configuration Parameters</h6>
                    <div class="grid md:grid-cols-2 gap-4 text-sm">
                      <div>
                        <code class="bg-gray-200 px-2 py-1 rounded">spark.shuffle.spill.compress</code>
                        <p class="text-xs mt-1">Compress spill files (default: true)</p>
                      </div>
                      <div>
                        <code class="bg-gray-200 px-2 py-1 rounded">spark.shuffle.compress</code>
                        <p class="text-xs mt-1">Compress shuffle output (default: true)</p>
                      </div>
                      <div>
                        <code class="bg-gray-200 px-2 py-1 rounded">spark.sql.shuffle.partitions</code>
                        <p class="text-xs mt-1">Reduce task count (default: 200)</p>
                      </div>
                      <div>
                        <code class="bg-gray-200 px-2 py-1 rounded">spark.shuffle.file.buffer</code>
                        <p class="text-xs mt-1">Buffer size for shuffle files (default: 32k)</p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Spillage Tuning -->
          <div id="spillage-tuning" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">3.4 Spillage Configuration and Tuning</h3>

            <div class="chart-container mb-8">
              <h4 class="font-inter text-xl font-semibold mb-4">Critical Configuration Parameters</h4>
              <div class="overflow-x-auto">
                <table class="w-full text-sm">
                  <thead>
                    <tr class="border-b border-gray-200">
                      <th class="text-left py-3 px-4 font-semibold">Parameter</th>
                      <th class="text-left py-3 px-4 font-semibold">Default</th>
                      <th class="text-left py-3 px-4 font-semibold">Impact</th>
                      <th class="text-left py-3 px-4 font-semibold">Tuning Guidance</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-gray-100">
                    <tr>
                      <td class="py-3 px-4 font-mono">spark.shuffle.spill.compress</td>
                      <td class="py-3 px-4">true</td>
                      <td class="py-3 px-4">Compress spill files</td>
                      <td class="py-3 px-4">Keep true unless CPU-bound; reduces I/O</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">spark.shuffle.compress</td>
                      <td class="py-3 px-4">true</td>
                      <td class="py-3 px-4">Compress shuffle output</td>
                      <td class="py-3 px-4">Keep true; significant network savings</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">spark.sql.shuffle.partitions</td>
                      <td class="py-3 px-4">200</td>
                      <td class="py-3 px-4">Reduce task count</td>
                      <td class="py-3 px-4">Increase for large data, decrease for small</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">spark.shuffle.file.buffer</td>
                      <td class="py-3 px-4">32k</td>
                      <td class="py-3 px-4">Buffer size for shuffle files</td>
                      <td class="py-3 px-4">Increase for large records</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">spark.shuffle.spill.initialMemoryThreshold</td>
                      <td class="py-3 px-4">5MB</td>
                      <td class="py-3 px-4">Trigger for shuffle spill</td>
                      <td class="py-3 px-4">Increase with more executor memory</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-mono">spark.memory.fraction</td>
                      <td class="py-3 px-4">0.6</td>
                      <td class="py-3 px-4">Unified memory pool size</td>
                      <td class="py-3 px-4">Adjust based on workload mix</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <p class="mt-4 text-sm text-gray-600">
                Source: <a href="https://spark.apache.org/docs/latest/sql-performance-tuning.html" class="citation">Spark SQL Performance Tuning Guide</a>
              </p>
            </div>

            <div class="grid md:grid-cols-2 gap-8">
              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Executor Memory Sizing and Partition Balance</h4>
                <p class="mb-4">
                  Effective spill prevention requires balancing <strong>executor memory</strong> against
                  <strong>partition data size</strong>. The fundamental constraint is that each task&#39;s working
                  set should fit comfortably in available execution memory.
                </p>

                <div class="code-block">
                  <pre># Sizing formula
Target partition size = Input data size / numPartitions
Memory per task = (executor memory × memory.fraction) / executor.cores

# Safety constraint
Target partition size &lt; Memory per task × safety_factor (0.5–0.7)</pre>
                </div>

                <div class="mt-4 p-3 bg-yellow-50 border-l-4 border-yellow-400 rounded">
                  <p class="text-sm text-yellow-800">
                    <strong>Example:</strong> 10GB input, 200 partitions → 50MB per partition.
                    8GB executor, 0.6 fraction, 4 cores → 1.2GB per task.
                    50MB easily fits, but 10× data growth would trigger spill.
                  </p>
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Off-Heap Memory and Buffer Tuning</h4>
                <p class="mb-4">
                  Spark supports <strong>off-heap memory allocation</strong> for execution and storage,
                  bypassing JVM heap management to reduce garbage collection pressure and enable larger
                  effective memory sizes.
                </p>

                <div class="space-y-3 text-sm">
                  <div class="flex items-center">
                    <code class="bg-gray-200 px-2 py-1 rounded mr-3">spark.memory.offHeap.enabled</code>
                    <span>Enable off-heap allocation</span>
                  </div>
                  <div class="flex items-center">
                    <code class="bg-gray-200 px-2 py-1 rounded mr-3">spark.memory.offHeap.size</code>
                    <span>Total off-heap bytes</span>
                  </div>
                  <div class="flex items-center">
                    <code class="bg-gray-200 px-2 py-1 rounded mr-3">spark.shuffle.io.preferDirectBufs</code>
                    <span>Direct ByteBuffers for network I/O</span>
                  </div>
                </div>

                <div class="mt-4 p-3 bg-blue-50 border-l-4 border-blue-400 rounded">
                  <p class="text-sm text-blue-800">
                    <strong>Benefit:</strong> Off-heap shuffle buffers reduce GC pressure during large shuffles,
                    particularly beneficial for workloads with high shuffle volume and sensitive latency requirements.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Code-Based Animations -->
      <section id="code-animations" class="py-16 bg-white">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="section-header">
            <h2 class="font-canela text-4xl font-bold text-primary mb-6">4. Code-Based Animations: Visualizing Spark Internals</h2>
          </div>

          <!-- Animation Framework -->
          <div id="animation-framework" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">4.1 Animation Framework Setup</h3>

            <div class="grid md:grid-cols-3 gap-6 mb-8">
              <div class="bg-gradient-to-br from-blue-50 to-blue-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-blue-900">Matplotlib Animation</h4>
                <p class="text-sm mb-3">Frame-based updates with efficient blitting for performance</p>
                <ul class="text-xs space-y-1 text-blue-800">
                  <li>• FuncAnimation for state transitions</li>
                  <li>• TimedAnimation for complex scenes</li>
                  <li>• State machine representation</li>
                  <li>• Interactive controls via widgets</li>
                </ul>
              </div>

              <div class="bg-gradient-to-br from-green-50 to-green-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-green-900">Manim Integration</h4>
                <p class="text-sm mb-3">Cinematic quality with precise geometric transformations</p>
                <ul class="text-xs space-y-1 text-green-800">
                  <li>• Mathematical animation engine</li>
                  <li>• Smooth node/edge animations</li>
                  <li>• Geometric partitioning visualization</li>
                  <li>• Area-proportional representations</li>
                </ul>
              </div>

              <div class="bg-gradient-to-br from-purple-50 to-purple-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-purple-900">Real-Time Components</h4>
                <p class="text-sm mb-3">Interactive visualization with dynamic state tracking</p>
                <ul class="text-xs space-y-1 text-purple-800">
                  <li>• Executor state visualization</li>
                  <li>• Partition location tracking</li>
                  <li>• Memory buffer monitoring</li>
                  <li>• Network flow simulation</li>
                </ul>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Core Visualization Components</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <h5 class="font-inter font-semibold mb-3">State Representation Framework</h5>
                  <div class="space-y-4">
                    <div>
                      <h6 class="font-semibold text-sm mb-1">Executors</h6>
                      <p class="text-xs text-gray-600">Rectangular nodes with CPU/memory indicators, active task count, disk I/O status</p>
                    </div>
                    <div>
                      <h6 class="font-semibold text-sm mb-1">Partitions</h6>
                      <p class="text-xs text-gray-600">Color-coded data blocks showing location, size, processing status, and dependencies</p>
                    </div>
                    <div>
                      <h6 class="font-semibold text-sm mb-1">Memory buffers</h6>
                      <p class="text-xs text-gray-600">Filling containers with level indicators, threshold markers, and spill warnings</p>
                    </div>
                  </div>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">Dynamic Visualization Elements</h5>
                  <div class="space-y-4">
                    <div>
                      <h6 class="font-semibold text-sm mb-1">Network flows</h6>
                      <p class="text-xs text-gray-600">Animated edges with packet indicators, transfer rate visualization, queue depth monitoring</p>
                    </div>
                    <div>
                      <h6 class="font-semibold text-sm mb-1">Disk files</h6>
                      <p class="text-xs text-gray-600">File icons with size labels, read/write status, spill file tracking, merge operations</p>
                    </div>
                    <div>
                      <h6 class="font-semibold text-sm mb-1">Interactive controls</h6>
                      <p class="text-xs text-gray-600">Playback speed, pause/step controls, parameter adjustment, detail drill-down</p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Shuffle Animation -->
          <div id="shuffle-animation" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">4.2 Shuffle Process Animation</h3>

            <div class="bg-white p-8 rounded-lg shadow-lg mb-8">
              <h4 class="font-inter text-xl font-semibold mb-6">Map Phase Visualization</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <h5 class="font-inter font-semibold mb-3">Data Partitioning and Bucket Assignment</h5>
                  <p class="mb-4">
                    The animation begins with <strong>input partitions distributed across executors</strong>,
                    visualized as colored blocks on node representations. Records flow through transformation
                    pipelines, with key extraction highlighting the grouping field.
                  </p>

                  <div class="space-y-2 text-sm">
                    <div class="flex items-center">
                      <span class="w-3 h-3 bg-blue-500 rounded-full mr-2"></span>
                      <span>Record processing and key extraction</span>
                    </div>
                    <div class="flex items-center">
                      <span class="w-3 h-3 bg-green-500 rounded-full mr-2"></span>
                      <span>Hash partitioning moment (dramatic jump visualization)</span>
                    </div>
                    <div class="flex items-center">
                      <span class="w-3 h-3 bg-purple-500 rounded-full mr-2"></span>
                      <span>Bucket accumulation with fill level indicators</span>
                    </div>
                  </div>
                </div>

                <div>
                  <img src="https://kimi-web-img.moonshot.cn/img/placeholder-0620/图片8.png" alt="Spark shuffle operation with data partitioning and network transfer" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" query="Apache Spark shuffle process animation" referrerpolicy="no-referrer" data-modified="1"/>
                </div>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg mb-8">
              <h4 class="font-inter text-xl font-semibold mb-6">Memory Buffer Filling and Spill Trigger</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <p class="mb-4">
                    The <strong>memory buffer visualization</strong> becomes critical as records accumulate.
                    A gauge shows memory consumption approaching threshold, with pulsing warnings before
                    dramatic spill animation.
                  </p>

                  <div class="space-y-3">
                    <div class="bg-red-50 p-3 rounded">
                      <h6 class="font-semibold text-red-900 mb-1">Threshold Breach</h6>
                      <p class="text-sm text-red-800">Memory gauge hits limit, &#34;SPILL&#34; alert appears</p>
                    </div>
                    <div class="bg-blue-50 p-3 rounded">
                      <h6 class="font-semibold text-blue-900 mb-1">Sorting Process</h6>
                      <p class="text-sm text-blue-800">Records reordered by partition ID for efficient writing</p>
                    </div>
                    <div class="bg-gray-50 p-3 rounded">
                      <h6 class="font-semibold text-gray-900 mb-1">Disk Write</h6>
                      <p class="text-sm text-gray-800">Mechanical animation showing slow disk vs. fast memory</p>
                    </div>
                  </div>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">File Generation Animation</h5>
                  <p class="mb-4">
                    The final map phase shows <strong>merge of spill files</strong> into consolidated output.
                    Spill file icons collapse into single data file with index file appearing alongside.
                  </p>

                  <div class="space-y-2 text-sm">
                    <div>• Spill file accumulation visualization</div>
                    <div>• Merge process with collapsing animation</div>
                    <div>• Index structure as lookup table visualization</div>
                    <div>• Final output as labeled package ready for reduce</div>
                  </div>
                </div>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Data Transfer and Reduce Phase Visualization</h4>

              <div class="grid md:grid-cols-3 gap-6">
                <div>
                  <h5 class="font-inter font-semibold mb-3">Network Flow Animation</h5>
                  <p class="text-sm mb-3">
                    Network connections form between map and reduce executors. Data packets flow with
                    size/color indicating compression and content type.
                  </p>
                  <ul class="text-xs space-y-1">
                    <li>• Representative sampling of M×R connections</li>
                    <li>• Parallel concurrent streams visualization</li>
                    <li>• Backpressure indication when buffers fill</li>
                    <li>• Local bypass optimization highlight</li>
                  </ul>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">Block Location Resolution</h5>
                  <p class="text-sm mb-3">
                    MapOutputTracker interaction visualized as central service responding to location queries,
                    separating metadata overhead from data transfer.
                  </p>
                  <ul class="text-xs space-y-1">
                    <li>• Two-step query and fetch process</li>
                    <li>• Metadata caching and batching benefits</li>
                    <li>• Scalability through centralized management</li>
                    <li>• Failure recovery visualization</li>
                  </ul>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">Reduce Task Execution</h5>
                  <p class="text-sm mb-3">
                    Incoming data streams merged through priority queue structure,
                    with streaming output beginning before all input received.
                  </p>
                  <ul class="text-xs space-y-1">
                    <li>• K-way merge algorithm visualization</li>
                    <li>• Key collision and combination animation</li>
                    <li>• Memory-bounded processing (merge heap size)</li>
                    <li>• Final output as consolidated partitions</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>

          <!-- Spillage Animation -->
          <div id="spillage-animation" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">4.3 Spillage Mechanism Animation</h3>

            <div class="bg-white p-8 rounded-lg shadow-lg mb-8">
              <h4 class="font-inter text-xl font-semibold mb-6">Memory Consumption Growth Visualization</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <p class="mb-4">
                    The animation focuses on <strong>memory pressure dynamics</strong> with time-series graph
                    showing memory allocation growth. Distinct curves track different data structures with
                    safety margin markings.
                  </p>

                  <div class="space-y-3">
                    <div class="flex items-center">
                      <span class="w-4 h-1 bg-blue-500 rounded mr-3"></span>
                      <span class="text-sm">Execution memory (shuffle sorting, hash tables)</span>
                    </div>
                    <div class="flex items-center">
                      <span class="w-4 h-1 bg-green-500 rounded mr-3"></span>
                      <span class="text-sm">Storage memory (cached RDDs, broadcast variables)</span>
                    </div>
                    <div class="flex items-center">
                      <span class="w-4 h-1 bg-red-500 rounded mr-3"></span>
                      <span class="text-sm">User memory (UDFs, user data structures)</span>
                    </div>
                  </div>
                </div>

                <div>
                  <img src="https://kimi-web-img.moonshot.cn/imagegen/20260209/021770637799257b85dd9573420c9c9dfb215f9da646d06927ff3_0.jpeg" alt="Animated visualization of memory usage and data spill in Apache Spark" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" query="Apache Spark memory spill animation" referrerpolicy="no-referrer" data-modified="1" data-score="0.00"/>
                </div>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg mb-8">
              <h4 class="font-inter text-xl font-semibold mb-6">Threshold Breach and Disk Write Trigger</h4>

              <div class="grid md:grid-cols-3 gap-6">
                <div class="bg-red-50 p-4 rounded-lg border border-red-200">
                  <h5 class="font-inter font-semibold mb-3 text-red-900">Critical Moment</h5>
                  <p class="text-sm text-red-800 mb-3">
                    Memory gauge hits limit, processing pauses, &#34;SPILL&#34; alert appears with dramatic visual effect
                  </p>
                  <ul class="text-xs space-y-1 text-red-700">
                    <li>• Pulsing warning indicators</li>
                    <li>• Processing pause animation</li>
                    <li>• Emergency response visualization</li>
                  </ul>
                </div>

                <div class="bg-blue-50 p-4 rounded-lg border border-blue-200">
                  <h5 class="font-inter font-semibold mb-3 text-blue-900">Serialization &amp; Sorting</h5>
                  <p class="text-sm text-blue-800 mb-3">
                    In-memory data being serialized (compression visualization) and sorted by partition ID
                  </p>
                  <ul class="text-xs space-y-1 text-blue-700">
                    <li>• Size reduction animation</li>
                    <li>• Reordering process visualization</li>
                    <li>• Temporary buffer clearing</li>
                  </ul>
                </div>

                <div class="bg-gray-50 p-4 rounded-lg border border-gray-200">
                  <h5 class="font-inter font-semibold mb-3 text-gray-900">Disk Write</h5>
                  <p class="text-sm text-gray-800 mb-3">
                    Mechanical animation showing slow disk I/O vs. fast memory access with timing contrast
                  </p>
                  <ul class="text-xs space-y-1 text-gray-700">
                    <li>• Disk write speed visualization</li>
                    <li>• Temporary file creation</li>
                    <li>• Multiple spill file accumulation</li>
                  </ul>
                </div>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Spill File Creation and Performance Impact</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <h5 class="font-inter font-semibold mb-3">Merge Process Visualization</h5>
                  <p class="mb-4">
                    Multiple spill cycles accumulate temporary file icons, then merge through external sort process.
                    The k-way merge algorithm parallels shuffle merge, reinforcing algorithmic patterns.
                  </p>

                  <div class="space-y-3">
                    <div class="flex items-start">
                      <i class="fas fa-layer-group text-blue-500 mr-2 mt-1"></i>
                      <span class="text-sm">Spill file accumulation visualization</span>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-compress-arrows-alt text-green-500 mr-2 mt-1"></i>
                      <span class="text-sm">External merge-sort algorithm demonstration</span>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-trash text-red-500 mr-2 mt-1"></i>
                      <span class="text-sm">Temporary file cleanup animation</span>
                    </div>
                  </div>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">Performance Impact Indicators</h5>
                  <p class="mb-4">
                    The animation concludes with <strong>performance metrics comparison</strong>:
                    execution time with and without spill, I/O volume, and memory efficiency.
                  </p>

                  <div class="bg-yellow-50 p-4 rounded">
                    <h6 class="font-semibold text-yellow-900 mb-2">Optimization Message</h6>
                    <p class="text-sm text-yellow-800">
                      Spill is functional but costly; tuning to avoid it yields significant benefits.
                      Visual comparison reinforces the performance cost of memory pressure.
                    </p>
                  </div>

                  <div class="mt-4 space-y-2 text-sm">
                    <div>• Execution time comparison (with/without spill)</div>
                    <div>• I/O volume visualization (disk vs. memory)</div>
                    <div>• Memory efficiency metrics</div>
                    <div>• Cost-benefit analysis of tuning efforts</div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Interactive DAG -->
          <div id="interactive-dag" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">4.4 Interactive DAG Execution Animator</h3>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Stage-by-Stage Execution Playback</h4>

              <div class="grid md:grid-cols-2 gap-8 mb-8">
                <div>
                  <p class="mb-4">
                    An interactive DAG animator enables <strong>stepping through execution</strong> at multiple
                    granularities: job level (stage dependencies), stage level (task distribution), and task level
                    (individual execution).
                  </p>

                  <div class="space-y-3">
                    <div class="bg-blue-50 p-3 rounded">
                      <h6 class="font-semibold text-blue-900 mb-1">Playback Controls</h6>
                      <ul class="text-sm text-blue-800 space-y-1">
                        <li>• Play/pause with speed adjustment</li>
                        <li>• Step forward/backward through stages</li>
                        <li>• Jump to specific execution points</li>
                        <li>• Slow motion for critical moments</li>
                      </ul>
                    </div>

                    <div class="bg-green-50 p-3 rounded">
                      <h6 class="font-semibold text-green-900 mb-1">Exploration Features</h6>
                      <ul class="text-sm text-green-800 space-y-1">
                        <li>• Pause at shuffle boundaries</li>
                        <li>• Adjust parameters to see effects</li>
                        <li>• Drill into task details</li>
                        <li>• View memory usage patterns</li>
                      </ul>
                    </div>
                  </div>
                </div>

                <div>
                  <img src="https://kimi-web-img.moonshot.cn/img/www.databricks.com/76cf2ef631440b977611b8abc672e26b82af5631.png" alt="Interactive Apache Spark DAG execution visualization" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" style="photo" query="Apache Spark DAG execution visualization" referrerpolicy="no-referrer" data-modified="1" data-score="0.00"/>
                </div>
              </div>

              <div class="grid md:grid-cols-3 gap-6">
                <div class="bg-gray-50 p-4 rounded-lg">
                  <h5 class="font-inter font-semibold mb-3">Task Distribution Tracking</h5>
                  <p class="text-sm mb-3">Task grid visualization with state transitions and straggler identification</p>
                  <ul class="text-xs space-y-1 text-gray-600">
                    <li>• Pending → Running → Completed color coding</li>
                    <li>• Failed task highlighting and retry visualization</li>
                    <li>• Straggler identification for slow tasks</li>
                    <li>• Real-time completion statistics</li>
                  </ul>
                </div>

                <div class="bg-gray-50 p-4 rounded-lg">
                  <h5 class="font-inter font-semibold mb-3">Shuffle Boundary Highlighting</h5>
                  <p class="text-sm mb-3">Critical shuffle points prominently marked with explanatory overlays</p>
                  <ul class="text-xs space-y-1 text-gray-600">
                    <li>• Map stage completion barrier visualization</li>
                    <li>• Shuffle data volume metrics display</li>
                    <li>• Reduce stage initiation animation</li>
                    <li>• Fundamental execution pattern reinforcement</li>
                  </ul>
                </div>

                <div class="bg-gray-50 p-4 rounded-lg">
                  <h5 class="font-inter font-semibold mb-3">Performance Metrics Integration</h5>
                  <p class="text-sm mb-3">Real-time metrics updating during playback with bottleneck detection</p>
                  <ul class="text-xs space-y-1 text-gray-600">
                    <li>• Tasks complete, records processed</li>
                    <li>• Shuffle data generated metrics</li>
                    <li>• Memory usage visualization</li>
                    <li>• Network I/O tracking</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Simple DAG Demo -->
      <section id="simple-dag-demo" class="py-16 bg-neutral">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="section-header">
            <h2 class="font-canela text-4xl font-bold text-primary mb-6">5. Simple DAG Demo: Wide Transformations in Practice</h2>
          </div>

          <!-- Demo Scenario -->
          <div id="demo-scenario" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">5.1 Problem Scenario: Employee Gender Salary Analysis</h3>

            <div class="grid md:grid-cols-2 gap-8 mb-8">
              <div>
                <h4 class="font-inter text-xl font-semibold mb-4">Dataset Structure</h4>
                <p class="mb-4">
                  The demonstration uses a simple employee dataset with the following structure,
                  <a href="https://bydatafix.com/2024/10/06/understanding-spark-dag-and-lazy-evaluation-for-enhanced-performance/" class="citation">loaded from CSV format</a>
                  with header row:
                </p>

                <div class="chart-container">
                  <div class="overflow-x-auto">
                    <table class="w-full text-sm">
                      <thead>
                        <tr class="border-b border-gray-200">
                          <th class="text-left py-2 px-3 font-semibold">Column</th>
                          <th class="text-left py-2 px-3 font-semibold">Type</th>
                          <th class="text-left py-2 px-3 font-semibold">Description</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-gray-100">
                        <tr>
                          <td class="py-2 px-3 font-mono">Gender</td>
                          <td class="py-2 px-3">String</td>
                          <td class="py-2 px-3">Employee gender category</td>
                        </tr>
                        <tr>
                          <td class="py-2 px-3 font-mono">Salary</td>
                          <td class="py-2 px-3">Numeric</td>
                          <td class="py-2 px-3">Employee compensation</td>
                        </tr>
                        <tr>
                          <td class="py-2 px-3 font-mono">Age</td>
                          <td class="py-2 px-3">Integer</td>
                          <td class="py-2 px-3">Employee age</td>
                        </tr>
                        <tr>
                          <td class="py-2 px-3 font-mono">Occupation</td>
                          <td class="py-2 px-3">String</td>
                          <td class="py-2 px-3">Job category</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>

                <div class="mt-4 p-3 bg-blue-50 border-l-4 border-blue-400 rounded">
                  <p class="text-sm text-blue-800">
                    <strong>Note:</strong> Data is initially distributed across partitions based on file size and
                    <code>spark.sql.files.maxPartitionBytes</code> (default 128MB).
                  </p>
                </div>
              </div>

              <div>
                <img src="https://kimi-web-img.moonshot.cn/img/placeholder-0620/图片16.png" alt="Sample employee dataset table with gender, salary, age, and occupation columns" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" style="photo" query="employee dataset table" referrerpolicy="no-referrer" data-modified="1"/>
              </div>
            </div>

            <div class="highlight-box">
              <h4 class="font-inter text-xl font-semibold mb-4">Analytical Goal: Average Salary by Gender</h4>
              <p class="mb-4">
                The objective is to compute <strong>average salary for each gender category</strong>, requiring:
              </p>

              <div class="grid md:grid-cols-3 gap-4">
                <div class="bg-white p-4 rounded-lg shadow">
                  <h5 class="font-semibold mb-2">1. Filtering</h5>
                  <p class="text-sm">Filter to relevant employee records (e.g., specific occupation and age range)</p>
                </div>
                <div class="bg-white p-4 rounded-lg shadow">
                  <h5 class="font-semibold mb-2">2. Grouping</h5>
                  <p class="text-sm">Group records by gender to colocate same-gender records</p>
                </div>
                <div class="bg-white p-4 rounded-lg shadow">
                  <h5 class="font-semibold mb-2">3. Aggregation</h5>
                  <p class="text-sm">Compute average salary within each gender group</p>
                </div>
              </div>

              <p class="mt-4">
                This analysis <strong>necessitates a wide transformation</strong> (
                <code>groupBy</code>) because records with the same gender may be distributed across
                multiple input partitions and must be colocated for aggregation.
              </p>
            </div>
          </div>

          <!-- PySpark Implementation -->
          <div id="pyspark-implementation" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">5.2 PySpark Code Implementation</h3>

            <div class="code-block mb-8">
              <pre>from pyspark.sql.functions import avg
import pyspark.sql.functions as F

# Action: Reading a file creates the initial DataFrame
df_data = spark.read.format(&#34;csv&#34;)\
        .option(&#34;header&#34;, &#34;true&#34;)\
        .load(&#39;/FileStore/employee.csv&#39;)

# Narrow transformation: Filter by Occupation
filtered_df = df_data.filter(&#34;Occupation=&#39;Teacher&#39;&#34;)

# Narrow transformation: Filter by Age range
filtered_age_df = filtered_df.filter(F.col(&#34;Age&#34;) &gt;= 25)\
                             .filter(F.col(&#34;Age&#34;) &lt;= 30)

# Wide transformation: Group by Gender and compute average salary
department_salary_df = filtered_age_df.groupBy(&#34;Gender&#34;)\
                                      .agg(avg(&#34;Salary&#34;))

# Action: Collect results and trigger execution
department_salary_df.show()</pre>
            </div>

            <div class="grid md:grid-cols-2 gap-8">
              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Initial Narrow Transformations</h4>
                <p class="mb-4">
                  The first two operations demonstrate <strong>narrow transformations</strong>—each filter
                  produces one output partition per input partition, with <a href="https://www.sparkplayground.com/tutorials/spark-theory/transformations-actions" class="citation">no data movement between executors</a>.
                </p>

                <div class="space-y-3">
                  <div class="bg-green-50 p-3 rounded">
                    <h6 class="font-semibold text-green-900 mb-1">Occupation Filter</h6>
                    <p class="text-sm text-green-800">Removes non-teacher records locally within each partition</p>
                  </div>
                  <div class="bg-blue-50 p-3 rounded">
                    <h6 class="font-semibold text-blue-900 mb-1">Age Range Filter</h6>
                    <p class="text-sm text-blue-800">Further narrows to employees aged 25-30</p>
                  </div>
                </div>

                <div class="mt-4 p-3 bg-green-50 border-l-4 border-green-400 rounded">
                  <p class="text-sm text-green-800">
                    <strong>Key Point:</strong> Both filter operations are pipelined into a single stage,
                    executing as fused tasks without intermediate materialization.
                  </p>
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Wide Transformation: groupBy with Aggregation</h4>
                <p class="mb-4">
                  The
                  <code>groupBy(&#34;Gender&#34;).agg(avg(&#34;Salary&#34;))</code> operation is a
                  <strong>wide transformation</strong> that triggers shuffle. Records with the same
                  <code>Gender</code> value may exist in different input partitions and must be redistributed.
                </p>

                <div class="bg-red-50 p-4 rounded border border-red-200">
                  <h6 class="font-semibold text-red-900 mb-2">Shuffle Trigger</h6>
                  <p class="text-sm text-red-800">
                    This operation ensures all records for a given gender are processed by the same reducer
                    task for correct aggregation, requiring all-to-all data movement across the cluster.
                  </p>
                </div>

                <div class="mt-4 space-y-2 text-sm">
                  <div>• Hash partitioning on Gender key</div>
                  <div>• Default 200 reducers from
                    <code>spark.sql.shuffle.partitions</code>
                  </div>
                  <div>• Partial aggregation for optimization</div>
                  <div>• Final average computation from sums and counts</div>
                </div>
              </div>
            </div>
          </div>

          <!-- Visual DAG -->
          <div id="visual-dag" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">5.3 Visual DAG Representation</h3>

            <div class="grid md:grid-cols-2 gap-8 mb-8">
              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Logical Plan: Narrow Transformations Pipeline</h4>
                <p class="mb-4">
                  The logical execution plan before optimization shows the transformation sequence
                  <a href="https://www.freecodecamp.org/news/how-to-optimize-pyspark-jobs-handbook/" class="citation">through Catalyst analysis</a>:
                </p>

                <div class="code-block">
                  <pre>Project [Gender, avg(Salary)]
+- Aggregate [Gender], [avg(Salary)]
   +- Filter ((Age &gt;= 25) AND (Age &lt;= 30))
      +- Filter (Occupation = &#39;Teacher&#39;)
         +- Relation [Gender,Salary,Age,Occupation] csv</pre>
                </div>

                <div class="mt-4 space-y-2 text-sm">
                  <div class="flex items-center">
                    <span class="w-3 h-3 bg-green-500 rounded-full mr-2"></span>
                    <span>Top to bottom: final projection to source data</span>
                  </div>
                  <div class="flex items-center">
                    <span class="w-3 h-3 bg-blue-500 rounded-full mr-2"></span>
                    <span>Two Filter operations pipelineable</span>
                  </div>
                  <div class="flex items-center">
                    <span class="w-3 h-3 bg-purple-500 rounded-full mr-2"></span>
                    <span>Aggregation requires grouping</span>
                  </div>
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Physical Plan with Exchange Operator</h4>
                <p class="mb-4">
                  The physical plan introduces stage boundaries at wide transformations with explicit
                  <strong>Exchange</strong> operator:
                </p>

                <div class="code-block">
                  <pre>*(1) HashAggregate(keys=[Gender#0], functions=[avg(Salary#1)])
+- Exchange hashpartitioning(Gender#0, 200), true, [id=#...]
   +- *(2) HashAggregate(keys=[Gender#0], functions=[partial_avg(Salary#1)])
      +- *(2) Filter ((Age#2 &gt;= 25) AND (Age#2 &lt;= 30))
         +- *(2) Filter (Occupation#3 = Teacher)
            +- FileScan csv [Gender#0,Salary#1,Age#2,Occupation#3] ...</pre>
                </div>

                <div class="mt-4 bg-red-50 p-3 rounded border border-red-200">
                  <p class="text-sm text-red-800">
                    <strong>Exchange Operator:</strong>
                    <code>Exchange hashpartitioning(Gender#0, 200)</code> indicates shuffle with
                    200 reducers, creating stage boundary.
                  </p>
                </div>
              </div>
            </div>

            <div class="chart-container">
              <h4 class="font-inter text-xl font-semibold mb-4">Execution Stage Breakdown</h4>
              <div class="overflow-x-auto">
                <table class="w-full text-sm">
                  <thead>
                    <tr class="border-b border-gray-200">
                      <th class="text-left py-3 px-4 font-semibold">Stage</th>
                      <th class="text-left py-3 px-4 font-semibold">Type</th>
                      <th class="text-left py-3 px-4 font-semibold">Operations</th>
                      <th class="text-left py-3 px-4 font-semibold">Data Flow</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-gray-100">
                    <tr>
                      <td class="py-3 px-4 font-medium">Stage 0 (Map)</td>
                      <td class="py-3 px-4"><span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs">Narrow</span></td>
                      <td class="py-3 px-4">CSV Read → Filter → Filter → Partial Aggregate</td>
                      <td class="py-3 px-4">File → Records → Filtered → Shuffle Write</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Shuffle</td>
                      <td class="py-3 px-4"><span class="bg-red-100 text-red-800 px-2 py-1 rounded text-xs">Wide</span></td>
                      <td class="py-3 px-4">HashPartitioning on Gender key</td>
                      <td class="py-3 px-4">Redistribute across 200 partitions</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Stage 1 (Reduce)</td>
                      <td class="py-3 px-4"><span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs">Narrow</span></td>
                      <td class="py-3 px-4">Shuffle Read → Merge → Final Aggregate → Project</td>
                      <td class="py-3 px-4">Fetch partitions → Combine → Compute avg → Output</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <!-- Execution Breakdown -->
          <div id="execution-breakdown" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">5.4 Execution Breakdown</h3>

            <div class="space-y-8">
              <div class="bg-white p-8 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Stage 0: Map-Side Operations</h4>

                <div class="grid md:grid-cols-2 gap-8">
                  <div>
                    <h5 class="font-inter font-semibold mb-3">Task Execution Flow</h5>
                    <div class="space-y-3">
                      <div class="flex items-start">
                        <span class="bg-blue-500 text-white rounded-full w-6 h-6 flex items-center justify-center text-xs mr-3 mt-0.5">1</span>
                        <div>
                          <div class="font-medium text-sm">CSV Reading</div>
                          <div class="text-xs text-gray-600">File parsing to record objects</div>
                        </div>
                      </div>
                      <div class="flex items-start">
                        <span class="bg-green-500 text-white rounded-full w-6 h-6 flex items-center justify-center text-xs mr-3 mt-0.5">2</span>
                        <div>
                          <div class="font-medium text-sm">Occupation Filter</div>
                          <div class="text-xs text-gray-600">Keep only &#39;Teacher&#39; records</div>
                        </div>
                      </div>
                      <div class="flex items-start">
                        <span class="bg-purple-500 text-white rounded-full w-6 h-6 flex items-center justify-center text-xs mr-3 mt-0.5">3</span>
                        <div>
                          <div class="font-medium text-sm">Age Range Filter</div>
                          <div class="text-xs text-gray-600">25-30 age constraint</div>
                        </div>
                      </div>
                      <div class="flex items-start">
                        <span class="bg-orange-500 text-white rounded-full w-6 h-6 flex items-center justify-center text-xs mr-3 mt-0.5">4</span>
                        <div>
                          <div class="font-medium text-sm">Partial Aggregation</div>
                          <div class="text-xs text-gray-600">Compute (sum, count) per partition</div>
                        </div>
                      </div>
                      <div class="flex items-start">
                        <span class="bg-red-500 text-white rounded-full w-6 h-6 flex items-center justify-center text-xs mr-3 mt-0.5">5</span>
                        <div>
                          <div class="font-medium text-sm">Shuffle Write</div>
                          <div class="text-xs text-gray-600">Hash-partition and write to disk</div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div>
                    <h5 class="font-inter font-semibold mb-3">Optimization Features</h5>
                    <div class="space-y-3">
                      <div class="bg-green-50 p-3 rounded">
                        <h6 class="font-semibold text-green-900 mb-1">Pipelined Execution</h6>
                        <p class="text-sm text-green-800">Multiple filters execute as single fused task without intermediate materialization</p>
                      </div>
                      <div class="bg-blue-50 p-3 rounded">
                        <h6 class="font-semibold text-blue-900 mb-1">Partial Aggregation</h6>
                        <p class="text-sm text-blue-800">Map-side combine reduces data volume before shuffle</p>
                      </div>
                      <div class="bg-purple-50 p-3 rounded">
                        <h6 class="font-semibold text-purple-900 mb-1">Partition Processing</h6>
                        <p class="text-sm text-purple-800">Each task processes its input partition independently</p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div class="bg-white p-8 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Shuffle: HashPartitioning on Gender Key</h4>

                <div class="grid md:grid-cols-2 gap-8">
                  <div>
                    <h5 class="font-inter font-semibold mb-3">Partitioning Logic</h5>
                    <div class="code-block mb-4">
                      <pre># For each record with Gender=g
target_partition = hash(g) % 200

# Write record to partition[target_partition]&#39;s shuffle file</pre>
                    </div>

                    <p class="mb-4">
                      All 200 reducers fetch their assigned partitions from every map output.
                      The shuffle read pattern requires each reducer to contact all mappers.
                    </p>

                    <div class="bg-yellow-50 p-3 rounded border-l-4 border-yellow-400">
                      <p class="text-sm text-yellow-800">
                        <strong>With 1000 mappers and 200 reducers:</strong> 200,000 fetch requests
                        in worst case, highlighting shuffle scalability challenges.
                      </p>
                    </div>
                  </div>

                  <div>
                    <h5 class="font-inter font-semibold mb-3">Shuffle File Structure</h5>
                    <p class="mb-4">
                      Each map task produces:
                    </p>
                    <ul class="space-y-2 text-sm">
                      <li class="flex items-start">
                        <i class="fas fa-file text-blue-500 mr-2 mt-1"></i>
                        <span><strong>Data file:</strong> Contains partitioned records</span>
                      </li>
                      <li class="flex items-start">
                        <i class="fas fa-list text-green-500 mr-2 mt-1"></i>
                        <span><strong>Index file:</strong> Byte offsets for each partition</span>
                      </li>
                    </ul>

                    <div class="mt-4 p-3 bg-blue-50 rounded">
                      <p class="text-sm text-blue-800">
                        <strong>Efficient Access:</strong> Index files enable precise ranged reads,
                        avoiding full file transfers and scan operations.
                      </p>
                    </div>
                  </div>
                </div>
              </div>

              <div class="bg-white p-8 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Stage 1: Reduce-Side Aggregation</h4>

                <div class="space-y-4">
                  <div class="grid md:grid-cols-3 gap-4">
                    <div class="bg-blue-50 p-4 rounded">
                      <h6 class="font-semibold text-blue-900 mb-2">Shuffle Read</h6>
                      <p class="text-sm text-blue-800">Fetch partition data from all map outputs using index files for precise access</p>
                    </div>
                    <div class="bg-green-50 p-4 rounded">
                      <h6 class="font-semibold text-green-900 mb-2">Merge-Sort</h6>
                      <p class="text-sm text-green-800">Combine records by Gender key using external merge-sort if ordering required</p>
                    </div>
                    <div class="bg-purple-50 p-4 rounded">
                      <h6 class="font-semibold text-purple-900 mb-2">Final Aggregate</h6>
                      <p class="text-sm text-purple-800">Compute sum(sum)/sum(count) for global average from partial aggregates</p>
                    </div>
                  </div>

                  <div class="bg-gray-50 p-4 rounded">
                    <h5 class="font-inter font-semibold mb-3">Final Average Calculation</h5>
                    <div class="code-block">
                      <pre># Average formula
avg = total_sum / total_count

# Where:
# total_sum = sum of all partial sums from map tasks
# total_count = sum of all partial counts from map tasks</pre>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Performance Analysis -->
          <div id="performance-analysis" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">5.5 Performance Characteristics</h3>

            <div class="grid md:grid-cols-2 gap-8 mb-8">
              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Data Movement Volume Estimation</h4>
                <p class="mb-4">
                  Shuffle data volume depends on several key factors that determine network and disk I/O requirements:
                </p>

                <div class="space-y-3">
                  <div class="flex items-start">
                    <i class="fas fa-filter text-blue-500 mr-2 mt-1"></i>
                    <div>
                      <div class="font-medium text-sm">Input Size After Filtering</div>
                      <div class="text-xs text-gray-600">Narrow transformations reduce data before shuffle</div>
                    </div>
                  </div>
                  <div class="flex items-start">
                    <i class="fas fa-compress text-green-500 mr-2 mt-1"></i>
                    <div>
                      <div class="font-medium text-sm">Partial Aggregation Effectiveness</div>
                      <div class="text-xs text-gray-600">Map-side combine reduces records shuffled</div>
                    </div>
                  </div>
                  <div class="flex items-start">
                    <i class="fas fa-key text-purple-500 mr-2 mt-1"></i>
                    <div>
                      <div class="font-medium text-sm">Key Cardinality</div>
                      <div class="text-xs text-gray-600">Number of unique Gender values (typically small)</div>
                    </div>
                  </div>
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Memory Requirements and Spill Risk</h4>
                <p class="mb-4">
                  Different operations present varying memory pressure and spill risk levels:
                </p>

                <div class="space-y-3">
                  <div class="bg-green-50 p-3 rounded border-l-4 border-green-400">
                    <h6 class="font-semibold text-green-900 mb-1">Map-side Filter (Low Risk)</h6>
                    <p class="text-sm text-green-800">Streaming processing with minimal memory footprint</p>
                  </div>
                  <div class="bg-yellow-50 p-3 rounded border-l-4 border-yellow-400">
                    <h6 class="font-semibold text-yellow-900 mb-1">Partial Aggregation (Medium Risk)</h6>
                    <p class="text-sm text-yellow-800">Small hash table (G entries) for map-side combine</p>
                  </div>
                  <div class="bg-red-50 p-3 rounded border-l-4 border-red-400">
                    <h6 class="font-semibold text-red-900 mb-1">Reduce-side Merge (High Risk)</h6>
                    <p class="text-sm text-red-800">Must buffer all map outputs for partition, high spill potential</p>
                  </div>
                </div>
              </div>
            </div>

            <div class="chart-container">
              <h4 class="font-inter text-xl font-semibold mb-4">Optimization Opportunities</h4>
              <div class="overflow-x-auto">
                <table class="w-full text-sm">
                  <thead>
                    <tr class="border-b border-gray-200">
                      <th class="text-left py-3 px-4 font-semibold">Technique</th>
                      <th class="text-left py-3 px-4 font-semibold">Application</th>
                      <th class="text-left py-3 px-4 font-semibold">Expected Benefit</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-gray-100">
                    <tr>
                      <td class="py-3 px-4 font-medium">Pre-aggregation</td>
                      <td class="py-3 px-4">Already applied via partial_avg</td>
                      <td class="py-3 px-4">Reduces shuffle records from O(N) to O(G × num_mappers)</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Partition tuning</td>
                      <td class="py-3 px-4">Reduce spark.sql.shuffle.partitions if G &lt;&lt; 200</td>
                      <td class="py-3 px-4">Fewer, larger reduce tasks reduce overhead</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Broadcast small tables</td>
                      <td class="py-3 px-4">Not applicable here (no join involved)</td>
                      <td class="py-3 px-4">N/A for this specific query pattern</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Adaptive Query Execution</td>
                      <td class="py-3 px-4">Enable spark.sql.adaptive.enabled</td>
                      <td class="py-3 px-4">Dynamically optimize partition count</td>
                    </tr>
                    <tr>
                      <td class="py-3 px-4 font-medium">Filter pushdown</td>
                      <td class="py-3 px-4">Already applied by Catalyst</td>
                      <td class="py-3 px-4">Reduces input data early in pipeline</td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <div class="mt-6 p-4 bg-blue-50 border-l-4 border-blue-400 rounded">
                <h5 class="font-semibold text-blue-900 mb-2">Specific Optimization for Gender Analysis</h5>
                <p class="text-sm text-blue-800">
                  For this query with low-cardinality grouping key (Gender: typically 2-3 values),
                  reducing shuffle partitions to 10-50 would improve performance by creating larger,
                  more efficient reduce tasks without excessive parallelism overhead.
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Optimization Strategies -->
      <section id="optimization-strategies" class="py-16 bg-white">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="section-header">
            <h2 class="font-canela text-4xl font-bold text-primary mb-6">6. Optimization Strategies and Best Practices</h2>
          </div>

          <!-- Minimizing Shuffle -->
          <div id="minimizing-shuffle" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">6.1 Minimizing Shuffle Impact</h3>

            <div class="grid md:grid-cols-3 gap-6 mb-8">
              <div class="bg-gradient-to-br from-blue-50 to-blue-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-blue-900">Partition Count Selection</h4>
                <p class="text-sm mb-3">Critical parameter: spark.sql.shuffle.partitions</p>
                <div class="space-y-2 text-xs text-blue-800">
                  <div>• Small data (&lt;1GB): 10-50 partitions</div>
                  <div>• Medium data (1-100GB): 100-400</div>
                  <div>• Large data (&gt;100GB): 400-2000+</div>
                  <div>• Skewed data: Use AQE dynamic adjustment</div>
                </div>
              </div>

              <div class="bg-gradient-to-br from-green-50 to-green-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-green-900">Map-Side Combiners</h4>
                <p class="text-sm mb-3">Automatic for sum, count, avg; ensure custom aggregations are associative</p>
                <div class="space-y-2 text-xs text-green-800">
                  <div>• Use reduceByKey over groupByKey</div>
                  <div>• aggregateByKey for explicit control</div>
                  <div>• combineByKey for complex aggregations</div>
                  <div>• Associative and commutative functions</div>
                </div>
              </div>

              <div class="bg-gradient-to-br from-purple-50 to-purple-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-purple-900">Broadcast Joins</h4>
                <p class="text-sm mb-3">Eliminate shuffle for small tables (&lt;10MB)</p>
                <div class="space-y-2 text-xs text-purple-800">
                  <div>• Broadcast small table to all executors</div>
                  <div>• Local hash join without data movement</div>
                  <div>• Use broadcast() function explicitly</div>
                  <div>• Threshold configurable via spark properties</div>
                </div>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Partition Count Selection Guide</h4>

              <div class="chart-container">
                <div class="overflow-x-auto">
                  <table class="w-full text-sm">
                    <thead>
                      <tr class="border-b border-gray-200">
                        <th class="text-left py-3 px-4 font-semibold">Scenario</th>
                        <th class="text-left py-3 px-4 font-semibold">Recommended Partitions</th>
                        <th class="text-left py-3 px-4 font-semibold">Rationale</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-gray-100">
                      <tr>
                        <td class="py-3 px-4">Small data (&lt;1GB), low cardinality</td>
                        <td class="py-3 px-4">10–50</td>
                        <td class="py-3 px-4">Avoid task overhead and scheduling latency</td>
                      </tr>
                      <tr>
                        <td class="py-3 px-4">Medium data (1–100GB), moderate cardinality</td>
                        <td class="py-3 px-4">100–400</td>
                        <td class="py-3 px-4">Default range, balanced parallelism</td>
                      </tr>
                      <tr>
                        <td class="py-3 px-4">Large data (&gt;100GB), high cardinality</td>
                        <td class="py-3 px-4">400–2000+</td>
                        <td class="py-3 px-4">Maintain parallelism for large datasets</td>
                      </tr>
                      <tr>
                        <td class="py-3 px-4">Skewed data</td>
                        <td class="py-3 px-4">Variable with AQE</td>
                        <td class="py-3 px-4">Dynamic adjustment handles data skew</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <div class="mt-6 grid md:grid-cols-2 gap-6">
                <div class="bg-red-50 p-4 rounded border-l-4 border-red-400">
                  <h5 class="font-semibold text-red-900 mb-2">Too Few Partitions</h5>
                  <ul class="text-sm text-red-800 space-y-1">
                    <li>• Memory pressure and spill risk</li>
                    <li>• Underutilized cluster resources</li>
                    <li>• Large task failure recovery cost</li>
                  </ul>
                </div>

                <div class="bg-red-50 p-4 rounded border-l-4 border-red-400">
                  <h5 class="font-semibold text-red-900 mb-2">Too Many Partitions</h5>
                  <ul class="text-sm text-red-800 space-y-1">
                    <li>• Task scheduling overhead</li>
                    <li>• Small-file problems</li>
                    <li>• NameNode pressure (HDFS)</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>

          <!-- Preventing Spillage -->
          <div id="preventing-spillage" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">6.2 Preventing Excessive Spillage</h3>

            <div class="grid md:grid-cols-2 gap-8 mb-8">
              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Executor Memory Right-Sizing</h4>
                <p class="mb-4">
                  General sizing guidelines for different workload types,
                  <a href="https://0x0fff.com/spark-architecture-shuffle/" class="citation">balancing memory vs. GC considerations</a>:
                </p>

                <div class="chart-container">
                  <div class="overflow-x-auto">
                    <table class="w-full text-sm">
                      <thead>
                        <tr class="border-b border-gray-200">
                          <th class="text-left py-3 px-4 font-semibold">Workload Type</th>
                          <th class="text-left py-3 px-4 font-semibold">Executor Memory</th>
                          <th class="text-left py-3 px-4 font-semibold">Cores</th>
                          <th class="text-left py-3 px-4 font-semibold">Notes</th>
                        </tr>
                      </thead>
                      <tbody class="divide-y divide-gray-100">
                        <tr>
                          <td class="py-3 px-4">Shuffle-heavy</td>
                          <td class="py-3 px-4">8–16GB</td>
                          <td class="py-3 px-4">4–8</td>
                          <td class="py-3 px-4">Balance memory vs. GC</td>
                        </tr>
                        <tr>
                          <td class="py-3 px-4">Cache-heavy</td>
                          <td class="py-3 px-4">16–32GB+</td>
                          <td class="py-3 px-4">2–4</td>
                          <td class="py-3 px-4">More memory for storage</td>
                        </tr>
                        <tr>
                          <td class="py-3 px-4">Mixed</td>
                          <td class="py-3 px-4">8GB</td>
                          <td class="py-3 px-4">4</td>
                          <td class="py-3 px-4">Common starting point</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>

                <div class="mt-4 p-3 bg-yellow-50 border-l-4 border-yellow-400 rounded">
                  <p class="text-sm text-yellow-800">
                    <strong>Important:</strong> Avoid executors &gt;32GB due to GC pauses;
                    prefer more smaller executors for large clusters.
                  </p>
                </div>
              </div>

              <div class="bg-white p-6 rounded-lg shadow-lg">
                <h4 class="font-inter text-xl font-semibold mb-4">Adaptive Query Execution (AQE)</h4>
                <p class="mb-4">
                  Spark 3.0+ AQE dynamically optimizes queries during execution,
                  <a href="https://ceur-ws.org/Vol-2608/paper31.pdf" class="citation">providing significant performance benefits</a>:
                </p>

                <div class="space-y-3">
                  <div class="bg-blue-50 p-3 rounded">
                    <h6 class="font-semibold text-blue-900 mb-1">Dynamic Coalesce</h6>
                    <p class="text-sm text-blue-800">Reduce small partition count post-shuffle automatically</p>
                  </div>
                  <div class="bg-green-50 p-3 rounded">
                    <h6 class="font-semibold text-green-900 mb-1">Join Strategy Switch</h6>
                    <p class="text-sm text-green-800">Broadcast small post-filter tables dynamically</p>
                  </div>
                  <div class="bg-purple-50 p-3 rounded">
                    <h6 class="font-semibold text-purple-900 mb-1">Skew Join Optimization</h6>
                    <p class="text-sm text-purple-800">Split skewed partitions automatically to balance workload</p>
                  </div>
                </div>

                <div class="code-block mt-4">
                  <pre># Enable AQE
spark.conf.set(&#34;spark.sql.adaptive.enabled&#34;, &#34;true&#34;)

# Additional AQE configurations
spark.conf.set(&#34;spark.sql.adaptive.coalescePartitions.enabled&#34;, &#34;true&#34;)
spark.conf.set(&#34;spark.sql.adaptive.skewJoin.enabled&#34;, &#34;true&#34;)</pre>
                </div>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Salting and Skew Mitigation Techniques</h4>

              <div class="grid md:grid-cols-3 gap-6">
                <div class="bg-blue-50 p-4 rounded-lg border border-blue-200">
                  <h5 class="font-inter font-semibold mb-3 text-blue-900">Salting</h5>
                  <p class="text-sm text-blue-800 mb-3">Add random prefix to key, aggregate twice for known skewed keys</p>
                  <div class="code-block text-xs">
                    <pre># Two-phase aggregation
df_salted = df.withColumn(&#34;salted_key&#34;, 
                         concat(col(&#34;gender&#34;), 
                                lit(&#34;_&#34;), 
                                (rand() * num_salts).cast(&#34;int&#34;)))

# First aggregation with salted key
temp_result = df_salted.groupBy(&#34;salted_key&#34;)\
                      .agg(sum(&#34;salary&#34;).alias(&#34;sum_salary&#34;),
                           count(&#34;*&#34;).alias(&#34;count_records&#34;))

# Second aggregation on original key
final_result = temp_result.groupBy(&#34;gender&#34;)\
                         .agg(sum(&#34;sum_salary&#34;)/sum(&#34;count_records&#34;))</pre>
                  </div>
                </div>

                <div class="bg-green-50 p-4 rounded-lg border border-green-200">
                  <h5 class="font-inter font-semibold mb-3 text-green-900">Adaptive Skew Join</h5>
                  <p class="text-sm text-green-800 mb-3">AQE automatic handling for unknown skew patterns</p>
                  <div class="space-y-2 text-xs text-green-700">
                    <div>• Detects skew at runtime automatically</div>
                    <div>• Splits skewed partitions dynamically</div>
                    <div>• Replicates small join side appropriately</div>
                    <div>• No manual intervention required</div>
                  </div>
                </div>

                <div class="bg-purple-50 p-4 rounded-lg border border-purple-200">
                  <h5 class="font-inter font-semibold mb-3 text-purple-900">Two-Phase Aggregation</h5>
                  <p class="text-sm text-purple-800 mb-3">Pre-aggregate with salted key, final aggregate for extreme skew</p>
                  <div class="space-y-2 text-xs text-purple-700">
                    <div>• First phase: Salted key aggregation</div>
                    <div>• Second phase: Original key aggregation</div>
                    <div>• Handles very high cardinality skew</div>
                    <div>• Balances load across all tasks</div>
                  </div>
                </div>
              </div>

              <div class="mt-6 p-4 bg-yellow-50 border-l-4 border-yellow-400 rounded">
                <h5 class="font-semibold text-yellow-900 mb-2">When to Use Each Technique</h5>
                <div class="grid md:grid-cols-3 gap-4 text-sm">
                  <div>
                    <strong class="text-yellow-800">Salting:</strong> Known skewed keys, manual control needed
                  </div>
                  <div>
                    <strong class="text-yellow-800">Adaptive Skew Join:</strong> Unknown skew patterns, automatic handling
                  </div>
                  <div>
                    <strong class="text-yellow-800">Two-Phase Aggregation:</strong> Extreme skew, very high cardinality
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Monitoring and Diagnostics -->
          <div id="monitoring-diagnostics" class="mb-16">
            <h3 class="font-canela text-2xl font-bold text-primary mb-8">6.3 Monitoring and Diagnostics</h3>

            <div class="grid md:grid-cols-3 gap-6 mb-8">
              <div class="bg-gradient-to-br from-blue-50 to-blue-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-blue-900">Spark UI Metrics</h4>
                <p class="text-sm mb-3">Shuffle Read/Write interpretation</p>
                <div class="space-y-2 text-xs text-blue-800">
                  <div>• Shuffle Read Size vs. input size</div>
                  <div>• Shuffle Write Size (partial aggregation effect)</div>
                  <div>• Spill (Memory) / Spill (Disk) indicators</div>
                  <div>• Task duration and GC metrics</div>
                </div>
              </div>

              <div class="bg-gradient-to-br from-green-50 to-green-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-green-900">Spill Identification</h4>
                <p class="text-sm mb-3">Task details analysis for spill detection</p>
                <div class="space-y-2 text-xs text-green-800">
                  <div>• &#34;Spill (Memory)&#34; column in Stage Details</div>
                  <div>• &#34;Spill (Disk)&#34; task-level metrics</div>
                  <div>• Event logs: SparkListenerTaskEnd</div>
                  <div>• Map vs. reduce spill pattern diagnosis</div>
                </div>
              </div>

              <div class="bg-gradient-to-br from-purple-50 to-purple-100 p-6 rounded-lg">
                <h4 class="font-inter text-lg font-semibold mb-3 text-purple-900">Event Log Analysis</h4>
                <p class="text-sm mb-3">Bottleneck detection through log analysis</p>
                <div class="space-y-2 text-xs text-purple-800">
                  <div>• SparkListenerStageSubmitted/Completed (timing)</div>
                  <div>• SparkListenerTaskStart/End (stragglers)</div>
                  <div>• SparkListenerExecutorAdded/Removed (scaling)</div>
                  <div>• Tools: History Server, Dr. Elephant</div>
                </div>
              </div>
            </div>

            <div class="bg-white p-8 rounded-lg shadow-lg">
              <h4 class="font-inter text-xl font-semibold mb-6">Spark UI: Shuffle Read/Write Metrics Interpretation</h4>

              <div class="grid md:grid-cols-2 gap-8">
                <div>
                  <h5 class="font-inter font-semibold mb-4">Healthy Indicators</h5>
                  <div class="space-y-3">
                    <div class="flex items-start">
                      <i class="fas fa-check-circle text-green-500 mr-2 mt-1"></i>
                      <div>
                        <div class="font-medium text-sm">Reasonable Shuffle Read vs. Input Size</div>
                        <div class="text-xs text-gray-600">Indicates effective partial aggregation</div>
                      </div>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-check-circle text-green-500 mr-2 mt-1"></i>
                      <div>
                        <div class="font-medium text-sm">Low Spill Metrics</div>
                        <div class="text-xs text-gray-600">Near zero memory/disk spill preferred</div>
                      </div>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-check-circle text-green-500 mr-2 mt-1"></i>
                      <div>
                        <div class="font-medium text-sm">Balanced Task Durations</div>
                        <div class="text-xs text-gray-600">Even distribution without stragglers</div>
                      </div>
                    </div>
                  </div>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-4">Warning Signs</h5>
                  <div class="space-y-3">
                    <div class="flex items-start">
                      <i class="fas fa-exclamation-triangle text-red-500 mr-2 mt-1"></i>
                      <div>
                        <div class="font-medium text-sm">High Spill Metrics</div>
                        <div class="text-xs text-gray-600">Indicates memory pressure requiring tuning</div>
                      </div>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-exclamation-triangle text-red-500 mr-2 mt-1"></i>
                      <div>
                        <div class="font-medium text-sm">Unbalanced Shuffle Read</div>
                        <div class="text-xs text-gray-600">Suggests data skew or ineffective aggregation</div>
                      </div>
                    </div>
                    <div class="flex items-start">
                      <i class="fas fa-exclamation-triangle text-red-500 mr-2 mt-1"></i>
                      <div>
                        <div class="font-medium text-sm">Task Duration Disparity</div>
                        <div class="text-xs text-gray-600">Sign of skew or resource contention</div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div class="mt-8 bg-gray-50 p-6 rounded">
                <h5 class="font-inter font-semibold mb-4">Event Log Analysis for Bottleneck Detection</h5>
                <p class="mb-4">
                  Spark event logs capture detailed execution data for post-hoc analysis with tools like
                  <a href="https://aws.plainenglish.io/spark-dag-visualization-8ee64f5a84e5" class="citation">Spark History Server</a>
                  and third-party analyzers.
                </p>

                <div class="grid md:grid-cols-3 gap-4">
                  <div>
                    <h6 class="font-semibold text-sm mb-2">Stage Events</h6>
                    <ul class="text-xs space-y-1 text-gray-600">
                      <li>• SparkListenerStageSubmitted</li>
                      <li>• SparkListenerStageCompleted</li>
                      <li>• Long stages indicate bottlenecks</li>
                      <li>• Dependency analysis for parallelism</li>
                    </ul>
                  </div>
                  <div>
                    <h6 class="font-semibold text-sm mb-2">Task Events</h6>
                    <ul class="text-xs space-y-1 text-gray-600">
                      <li>• SparkListenerTaskStart</li>
                      <li>• SparkListenerTaskEnd</li>
                      <li>• Straggler identification</li>
                      <li>• Spill pattern analysis</li>
                    </ul>
                  </div>
                  <div>
                    <h6 class="font-semibold text-sm mb-2">Resource Events</h6>
                    <ul class="text-xs space-y-1 text-gray-600">
                      <li>• SparkListenerExecutorAdded</li>
                      <li>• SparkListenerExecutorRemoved</li>
                      <li>• Dynamic allocation behavior</li>
                      <li>• Resource contention patterns</li>
                    </ul>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Footer -->
      <footer class="py-16 bg-primary text-white">
        <div class="container mx-auto px-8 max-w-6xl">
          <div class="grid md:grid-cols-3 gap-8">
            <div>
              <h3 class="font-canela text-2xl font-bold mb-4">Key Takeaways</h3>
              <ul class="space-y-2 text-sm">
                <li>• Shuffles are the primary performance bottleneck in Spark</li>
                <li>• Spillage indicates memory pressure and misconfiguration</li>
                <li>• Unified memory management enables dynamic optimization</li>
                <li>• Partition count tuning is critical for performance</li>
                <li>• AQE provides automatic optimization capabilities</li>
              </ul>
            </div>

            <div>
              <h3 class="font-canela text-2xl font-bold mb-4">Further Reading</h3>
              <ul class="space-y-2 text-sm">
                <li>
                  <a href="https://spark.apache.org/docs/latest/" class="citation">Apache Spark Official Documentation</a>
                </li>
                <li>
                  <a href="https://spark.apache.org/docs/latest/sql-performance-tuning.html" class="citation">Spark SQL Performance Tuning</a>
                </li>
                <li>
                  <a href="https://0x0fff.com/spark-architecture-shuffle/" class="citation">Spark Architecture and Shuffle</a>
                </li>
              </ul>
            </div>

            <div>
              <h3 class="font-canela text-2xl font-bold mb-4">Tools &amp; Resources</h3>
              <ul class="space-y-2 text-sm">
                <li>• Spark History Server for performance analysis</li>
                <li>• Dr. Elephant for optimization recommendations</li>
                <li>• Sparklens for resource planning</li>
                <li>• Ganglia for cluster monitoring</li>
              </ul>
            </div>
          </div>

          <div class="border-t border-gray-600 mt-12 pt-8 text-center text-sm">
            <p>© 2024 Apache Spark End-to-End Analysis. Comprehensive guide to shuffle mechanics, memory management, and optimization strategies.</p>
          </div>
        </div>
      </footer>
    </main>

    <script>
        // Smooth scrolling for navigation links
        document.querySelectorAll('.toc-link').forEach(link => {
            link.addEventListener('click', function(e) {
                e.preventDefault();
                const targetId = this.getAttribute('href').substring(1);
                const targetElement = document.getElementById(targetId);
                if (targetElement) {
                    targetElement.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            });
        });

        // Active link highlighting
        window.addEventListener('scroll', function() {
            const sections = document.querySelectorAll('section[id]');
            const scrollPos = window.scrollY + 100;

            sections.forEach(section => {
                const top = section.offsetTop;
                const bottom = top + section.offsetHeight;
                const id = section.getAttribute('id');
                const link = document.querySelector(`.toc-link[href="#${id}"]`);

                if (scrollPos >= top && scrollPos <= bottom) {
                    document.querySelectorAll('.toc-link').forEach(l => l.classList.remove('active'));
                    if (link) link.classList.add('active');
                }
            });
        });

        // Intersection Observer for better scroll detection
        const observerOptions = {
            root: null,
            rootMargin: '-20% 0px -80% 0px',
            threshold: 0
        };

        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const id = entry.target.getAttribute('id');
                    document.querySelectorAll('.toc-link').forEach(link => {
                        link.classList.remove('active');
                    });
                    const activeLink = document.querySelector(`.toc-link[href="#${id}"]`);
                    if (activeLink) activeLink.classList.add('active');
                }
            });
        }, observerOptions);

        document.querySelectorAll('section[id]').forEach(section => {
            observer.observe(section);
        });
    </script>
  

</body></html>
