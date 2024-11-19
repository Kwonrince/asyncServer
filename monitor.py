import psutil
import time
import os
from datetime import datetime

def get_python_processes():
    python_processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_percent']):
        try:
            if 'python' in proc.name().lower():
                python_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return python_processes

def monitor_python_processes():
    while True:
        os.system('clear')  # 화면 클리어
        processes = get_python_processes()
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n=== Python Processes Monitor at {current_time} ===\n")
        
        # 헤더 출력
        print(f"{'PID':>8} {'CPU %':>8} {'MEM %':>8} {'MEM(MB)':>10} {'THREADS':>8} {'COMMAND':>50}")
        print("-" * 92)
        
        # 각 프로세스 정보 출력
        for proc in processes:
            try:
                with proc.oneshot():
                    # 메모리 정보 (MB 단위로 변환)
                    mem_mb = proc.memory_info().rss / 1024 / 1024
                    
                    # 커맨드라인 가져오기
                    try:
                        cmd = ' '.join(proc.cmdline())
                        if len(cmd) > 500:
                            cmd = cmd[:497] + "..."
                    except:
                        cmd = proc.name()
                    
                    # 정보 출력
                    print(f"{proc.pid:>8} {proc.cpu_percent():>8.1f} "
                          f"{proc.memory_percent():>8.1f} {mem_mb:>10.1f} "
                          f"{proc.num_threads():>8} {cmd:<50}")
            
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            except Exception as e:
                print(f"Error getting process info: {e}")
        
        # 전체 요약 정보
        total_cpu = sum(proc.cpu_percent() for proc in processes)
        total_mem = sum(proc.memory_percent() for proc in processes)
        
        print("\n=== Summary ===")
        print(f"Total Python Processes: {len(processes)}")
        print(f"Total CPU Usage: {total_cpu:.1f}%")
        print(f"Total Memory Usage: {total_mem:.1f}%")
        
        time.sleep(1)  # 1초마다 업데이트

if __name__ == "__main__":
    try:
        monitor_python_processes()
    except KeyboardInterrupt:
        print("\n모니터링을 종료합니다.")