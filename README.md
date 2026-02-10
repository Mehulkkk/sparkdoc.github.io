<html lang="en">

  <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Canela:wght@300;400;700&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
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
          <a href="#stages-tasks" class="toc-link text-sm">1.2 Stages & Tasks</a>
          <a href="#dag-overview" class="toc-link text-sm">1.3 DAG Structure</a>
        </div>

        <a href="#understanding-shuffles" class="toc-link">2. Understanding Spark Shuffles</a>
        <div class="ml-4 space-y-1">
          <a href="#shuffle-core" class="toc-link text-sm">2.1 Core Concepts</a>
          <a href="#shuffle-mechanics" class="toc-link text-sm">2.2 Complete Lifecycle</a>
          <a href="#shuffle-evolution" class="toc-link text-sm">2.3 Manager Evolution</a>
        </div>

        <a href="#memory-management" class="toc-link">3. Memory Management & Spillage</a>
        <div class="ml-4 space-y-1">
          <a href="#memory-architecture" class="toc-link text-sm">3.1 Unified Memory</a>
          <a href="#spillage-definition" class="toc-link text-sm">3.2 What is Spillage?</a>
          <a href="#spillage-triggers" class="toc-link text-sm">3.3 Triggers & Mechanisms</a>
          <a href="#spillage-tuning" class="toc-link text-sm">3.4 Configuration & Tuning</a>
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
          <a href="#monitoring-diagnostics" class="toc-link text-sm">6.3 Monitoring & Diagnostics</a>
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
                <br>
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
              Apache Spark's shuffle mechanism represents the <strong>most critical performance bottleneck</strong>
              in distributed data processing. This comprehensive analysis reveals that shuffle operations—
              triggered by key-based transformations like
              <code>groupBy</code> and
              <code>join</code>—involve
              complex all-to-all data redistribution across cluster nodes, consuming significant network,
              disk, and memory resources.
            </p>

            <p class="text-lg leading-relaxed mb-6">
              The research demonstrates that <strong>spillage</strong>, Spark's emergency response to memory
              pressure, fundamentally differs from intentional shuffle operations. While shuffle is a
              planned computational pattern, spill represents misconfiguration that can degrade performance
              by 10-100x through unnecessary disk I/O operations.
            </p>

            <p class="text-lg leading-relaxed">
              Modern Spark's <a href="https://zhmin.github.io/posts/spark-shuffle-sort-writer-2/" class="citation">sort-based shuffle implementation</a>
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
                <img src="https://kimi-img.moonshot.cn/pub/icon/spinner.svg" alt="Apache Spark driver and executors cluster architecture" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" query="Apache Spark cluster architecture diagram" referrerpolicy="no-referrer" />
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
                Spark's <strong>lazy evaluation</strong> model defers computation until an action is invoked,
                recording transformations in a <strong>DAG</strong> that represents the complete computation plan.
                Each RDD maintains <a href="https://stackoverflow.com/questions/25836316/how-dag-works-under-the-covers-in-rdd" class="citation">pointers to parent RDDs</a>
                with transformation metadata—this lineage enables both optimization and fault recovery.
              </p>

              <p>
                The DAG structure ensures no circular dependencies exist: transformations always move forward
                from source data to derived results, never cycling back. This immutability and lineage
                provide the foundation for Spark's fault tolerance through recomputation.
              </p>
            </div>

            <div class="grid md:grid-cols-2 gap-8">
              <div>
                <h4 class="font-inter text-xl font-semibold mb-4">Logical to Physical Plan Transformation</h4>
                <p class="mb-4">
                  Spark's query execution proceeds through multiple planning phases. The <strong>logical plan</strong>
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
                <img src="https://kimi-img.moonshot.cn/pub/icon/spinner.svg" alt="Spark query plan transformation from logical to physical with optimization stages" class="w-full rounded-lg shadow-lg" size="medium" aspect="wide" style="linedrawing" query="Apache Spark query plan transformation diagram" referrerpolicy="no-referrer" />
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
                every record from every map task must reach the correct reduce task based on its key's hash value.
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
                recording byte offsets for each partition's data. This design consolidates what would be
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
                  Spark's original hash shuffle produced <strong>M × R shuffle files</strong>—a quadratic
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
                      <li>• Executor loss doesn't invalidate shuffle data</li>
                      <li>• Supports dynamic allocation and scaling</li>
                      <li>• Essential for cloud deployments with preemption</li>
                      <li>• Independent process from executor JVM</li>
                    </ul>
                  </div>
                </div>

                <div>
                  <h5 class="font-inter font-semibold mb-3">Magnet: Push-Merge Shuffle</h5>
                  <p class="mb-4 text-sm">
                    LinkedIn's <strong>Magnet</strong> project extends ESS with push-merge shuffle,
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
                <strong>Spillage</strong> occurs when Spark's in-memory data structures exceed available memory,
                forcing data to be written to disk to prevent <strong>OutOfMemory errors</strong>. Unlike shuffle—which
                is intentional data movement—spill is an <strong>emergency response to memory pressure</strong>.
              </p>

              <p>
                <a href="https://stackoverflow.com/questions/37848182/understanding-spark-shuffle-spill" class="citation">Spill represents performance degradation</a>:
                disk I/O is orders of magnitude slower than memory access, and spilled data must eventually be read back,
                doubling the I/O co
