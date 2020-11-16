(defproject com.nedap.staffing-solutions/datomic-tx-metrics "0.3.0-alpha1"
  :description "Containing a callback handler for collecting Datomic Transactor + JVM metrics for consumption (e.g. by Prometheus) using a web endpoint offered by the included web server."
  :dependencies [[aleph "0.4.6"]
                 [bidi "2.1.6"]
                 [com.taoensso/timbre "4.10.0"]
                 [environ "1.1.0"]
                 [io.prometheus/simpleclient "0.6.0" #_"transitive"]
                 [io.prometheus/simpleclient_hotspot "0.5.0"]
                 [org.clojure/clojure "1.10.1"]
                 [prom-metrics "0.5-alpha2"]
                 [ring/ring-core "1.7.1"]]

  :signing {:gpg-key "releases-staffingsolutions@nedap.com"}

  :repositories {"releases" {:url      "https://nedap.jfrog.io/nedap/staffing-solutions/"
                             :username :env/artifactory_user
                             :password :env/artifactory_pass}}

  :repository-auth {#"https://nedap.jfrog\.io/nedap/staffing-solutions/"
                    {:username :env/artifactory_user
                     :password :env/artifactory_pass}}

  :profiles {:ncrw       {:global-vars  {*assert* true} ;; `ci.release-workflow` relies on runtime assertions
                          :dependencies [[com.nedap.staffing-solutions/ci.release-workflow "1.11.0"]]}

             :ci       {:pedantic?    :abort
                        :jvm-opts     ["-Dclojure.main.report=stderr"]}})
