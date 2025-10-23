from hatchling.builders.hooks.plugin.interface import BuildHookInterface
import os
import subprocess
import shutil

class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        outPath = "src/gensql/gateway.jar"
        inPath = "gateway"
        midPath = "gateway/target/gateway.jar"

        # Check if Jar needs to be rebuilt
        needsRebuild = False
        if not os.path.exists(outPath):
            needsRebuild = True
        else:
            outTime = os.path.getmtime(outPath)
            for root, _, files in os.walk(inPath):
                for file in files:
                    if os.path.getmtime(os.path.join(root, file)) > outTime:
                        needsRebuild = True
                        break
        
        # Build Jar
        if needsRebuild:
            subprocess.run(["clojure", "-T:build", "uber"], check=True, cwd=inPath)
            shutil.copy(midPath, outPath)
