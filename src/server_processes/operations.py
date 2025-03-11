from . import *

def operation_functions(app, SessionDep):
    @app.post("/delete/{prop}")
    async def delete(
        prop: str,
        session: SessionDep,
    ):
        try:
            the_object=object_mapper[prop]
            entries= session.exec( select(the_object) ).all()
            for entry in entries:
                session.delete(entry)
            session.commit()
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not execute delete for {prop}: {analyse_exception(ex)}")

    @app.post("/clean_pending/{prop}")
    async def clean_double(
        prop: str,
        session: SessionDep,
        force:bool=False,
    ):
        try:
            the_object=object_mapper[prop]
            pending_status_key=-1
            the_pending=session.exec(
                select(the_object)
                .where(the_object.converged==pending_status_key)
            ).all()
            [ session.delete(x) for x in the_pending ]
            session.commit()
            message=f"Deleted {len(the_pending)} objects of type {the_object.__name__}"
            return {'message':message}
        except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR,f"Could not execute {clean_double}: {analyse_exception(ex)}")

    @app.post("/clean_double/{prop}")
    async def clean_double(
        prop: str,
        session: SessionDep,
        force:bool=False,
    ):
        try:
            the_object=object_mapper[prop]
            if the_object==IsoDens_Surface:
                #entries = [ x for x in
                #           session.exec( select(the_object.id, the_object.wave_function_id,the_object.iso_density, the_object.spacing, the_object.converged)).all()]
                entries = [ x for x in
                           session.exec( select(the_object.id, the_object.wave_function_id,the_object.iso_density, the_object.spacing, the_object.converged)).all()]
                
                # see all entries where iso_density and spacing is the same
                iso_densities=list(set([ e[2] for e in entries]))
                spacing=list(set([ e[3] for e in entries]))
                
                pair_dic=dict([ (iso, dict([  (x,[]) for x in spacing  ])) for iso in iso_densities])

                data=[ [e[2],e[3]] for e in entries]
                for i, (iso_d,spac) in enumerate(data):
                    pair_dic[iso_d][spac].append(i)

                for isod in iso_densities:
                    for spac in spacing:
                        try:
                            ids=[ (entries[ind][0],entries[ind][1], entries[ind][4]) for ind in pair_dic[isod][spac]]
                        except:
                            raise Exception(f"{isod} {spac} {dic.keys()} {iso_densities}")
                        dic={}
                        for isod_id, wfn_id, converged in ids:
                            if not wfn_id in dic.keys():
                                dic.update({wfn_id:[]})
                            dic[wfn_id].append( (isod_id, converged) )
                        for wfn_id, row in dic.items():
                            converged=[ x[0] for x in row if x[1]==1]
                            not_converged=[ x[0] for x in row if x[1]!=1]
                            if len(converged)==0:
                                pass
                            elif len(converged)>1:
                                if force:
                                    # remove all except the lowest id
                                    delete_ids=sorted(converged)[1:]
                                    assert len(delete_ids)+1==len(converged),f"Problem in deleting, check failed."
                                    [ 
                                        session.delete( session.get(the_object,isod_id)) 
                                        for isod_id  in delete_ids
                                    ]
                                    session.commit()
                                else:    
                                    raise Exception(f"Found more than one converged item (remove all except smallest id)")
            elif type(the_object)==type(RHO_ESP_Map):
                entries=session.exec( select(the_object.id, the_object.surface_id, the_object.converged) ).all()
                dic={}
                for id, surf_id, conv in entries:
                    if not surf_id in dic.keys():
                        dic.update({surf_id:[]})
                    dic[surf_id].append( (id, conv))
                for surf_id, vals in dic.items():
                    if len(vals)>1:
                        converged=[ x[0] for x in vals if x[1]==1]
                        pending=[ x[0] for x in vals if x[1]==-1]
                        assert len(converged)<2
                        if len(converged)>0:
                            [ session.delete(session.get(the_object,the_id)) for the_id in pending ]
                session.commit()


                                
                            
                                
                            



                


        except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR,
            f"Couldn noe execute {clean_double}: {analyse_exception(ex)}")

