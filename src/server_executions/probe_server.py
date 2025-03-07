
from . import *
import tqdm
import sys
from http import HTTPStatus

def human_time_duration(seconds:float):
    ''' Convert seconds (duration) to human readable string 
    
    from https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3
    '''
    
    if seconds<1.:
        return f'{seconds*1000:.3g} ms'
    if seconds<10.:
        return f'{seconds:.3g} s'
        
    TIME_DURATION_UNITS = (
      ("week","s", 60 * 60 * 24 * 7),
      ("day","s", 60 * 60 * 24),
      ("h","", 60 * 60),
      ("min","", 60),
      ("s","", 1),
    )
    parts = []
    for unit, plur, div in TIME_DURATION_UNITS:
        amount, seconds = divmod(int(seconds), div)
        if amount > 0:
            parts.append(f"{amount} {unit}{plur if amount > 1 else ''}")
    return " ".join(parts)


def probe_server(address, delay, refresh, property, method):

    request=f"{address}/info/{property}?delay={delay}"
    response = requests.get(request)

    status_code=response.status_code
    if status_code != HTTPStatus.OK:
        raise ValueError(f"Error getting initial response for \'{request}\': code={status_code}, details={response.text}")

    body = response.json()
    pending = body["pending"]
    converged = body["converged"]
    failed = body["failed"]
    total = pending + converged + failed
    processed = converged + failed
    if refresh <=0:
        percent = 100 * processed / total
        recent_worker = body["recently_active_workers"]
        print(f'[{percent:.2f}%] {processed}/{total} (r,f,w = {pending},{failed},{recent_worker})')
        return
    
    try:
        with tqdm.tqdm(total=total, initial=processed, dynamic_ncols=True) as pbar:
            while True:
                response = requests.get(request)
                if response.status_code != 200:
                    time.sleep(refresh)
                    continue

                body = response.json()
                pending = body["pending"]
                converged = body["converged"]
                failed = body["failed"]
                added = converged + failed - processed
                pbar.total = pending + converged + failed
                recent_worker = body["recently_active_workers"]
                pbar.set_postfix(
                    {
                        "r,f,w": (pending,failed,recent_worker),
                    }
                )
                if added > 0:
                    pbar.update(added)
                    processed = converged + failed
                if pending <= 0:
                    break
                time.sleep(refresh)
    except KeyboardInterrupt:
        pass